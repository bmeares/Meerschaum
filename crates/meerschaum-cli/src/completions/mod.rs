//! Shell completions for the `mrsm` / `meerschaum` programs (v5).
//!
//! The scripts that `mrsm show completions <shell>` prints (and `mrsm install completions`
//! writes) hold no candidates: on every `<TAB>` they run the program again with
//! `_MRSM_COMPLETE=<shell>` and the command line typed so far, and the program answers at the very
//! top of [`crate::run`], before it loads logging, plugins or a runtime (see
//! [`answer_if_requested`]). Nothing is run, written, created or downloaded for an answer: the
//! config registry is read with writes off (a missing root stays missing), the plugin manifest is
//! the cached file as the last command left it (never rebuilt, no plugin code imported) and no
//! worker or Python process is started.
//!
//! The candidates are the interactive shell's: [`CompletionData::get_completions`] answers the
//! positional words (actions, subactions, connector labels, job and plugin names), over the same
//! data the shell reads. This module adds what a command line has and the shell prompt does not:
//! flags anywhere, flag values (`-i`, `-c`, `-r`, `-e`, flags with `choices`, files for the path
//! flags) and `--flag=value`.
//!
//! # Protocol
//!
//! Environment of the call (the scripts set it for that one process only):
//!
//! - `_MRSM_COMPLETE=bash|zsh|fish|powershell`: asks for an answer.
//! - `_MRSM_COMPLETE_LINE`: the command line up to the cursor, shell-quoted (bash). The program
//!   splits it with POSIX quoting rules: spaces inside quotes, backslash escapes.
//! - or `_MRSM_COMPLETE_WORDS`: the words already unquoted by the shell, separated by `U+001F`
//!   (zsh, fish, PowerShell). Either way the first word is the program and the last is the word
//!   being completed (empty after a space).
//!
//! Answer on stdout, one item per line: the kind, then the items.
//!
//! ```text
//! candidates
//! show
//! sh
//! ```
//!
//! `candidates` is followed by one replacement for the current word per line, optionally
//! `<TAB><description>`; `files` (no items) asks the script for the shell's own file completion.
//! Exit code 0 always.

use std::collections::BTreeSet;
use std::io::Write;

use indexmap::IndexMap;

use crate::actions::CORE_ACTIONS;
use crate::shell::completer::{
    Completion, CompletionData, INSTANCE_TYPES, cli_do_names, connectors_from_config,
};
use crate::spec::{self, ArgType, Nargs};
use crate::types::DateContext;

pub mod install;

/// The environment variable that asks for an answer (its value is the shell).
pub const ENV_SHELL: &str = "_MRSM_COMPLETE";
/// The command line up to the cursor, shell-quoted.
pub const ENV_LINE: &str = "_MRSM_COMPLETE_LINE";
/// The unquoted words up to the cursor, separated by [`WORD_SEPARATOR`].
pub const ENV_WORDS: &str = "_MRSM_COMPLETE_WORDS";
/// Separates the words of [`ENV_WORDS`] (the ASCII unit separator).
pub const WORD_SEPARATOR: char = '\u{1f}';

/// The shells with a script, in the order the docs list them.
pub const SHELL_NAMES: &[&str] = &["bash", "zsh", "fish", "powershell"];

/// A shell with a completion script.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shell {
    /// GNU Bash (3.2 and later).
    Bash,
    /// Zsh.
    Zsh,
    /// Fish.
    Fish,
    /// PowerShell (Windows PowerShell 5.1 and PowerShell 7+).
    PowerShell,
}

impl Shell {
    /// The name the commands take (`pwsh` and `ps` are aliases of `powershell`).
    pub fn parse(name: &str) -> Option<Shell> {
        match name.trim().to_ascii_lowercase().as_str() {
            "bash" => Some(Shell::Bash),
            "zsh" => Some(Shell::Zsh),
            "fish" => Some(Shell::Fish),
            "powershell" | "pwsh" | "ps" => Some(Shell::PowerShell),
            _ => None,
        }
    }

    /// The canonical name.
    pub fn name(self) -> &'static str {
        match self {
            Shell::Bash => "bash",
            Shell::Zsh => "zsh",
            Shell::Fish => "fish",
            Shell::PowerShell => "powershell",
        }
    }

    /// The shell the user runs: `$SHELL`'s program name, else PowerShell on Windows.
    pub fn detect(shell_var: Option<&str>) -> Option<Shell> {
        if let Some(path) = shell_var.filter(|s| !s.trim().is_empty()) {
            let base = path
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or("")
                .trim_end_matches(".exe");
            return Shell::parse(base);
        }
        cfg!(windows).then_some(Shell::PowerShell)
    }
}

/// One candidate: the replacement for the current word and an optional one-line description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// What replaces the word being completed.
    pub text: String,
    /// Shown next to the candidate by shells that have descriptions (zsh, fish, PowerShell).
    pub description: Option<String>,
}

impl Candidate {
    fn plain(text: impl Into<String>) -> Candidate {
        Candidate {
            text: text.into(),
            description: None,
        }
    }
}

/// The answer to a completion request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Answer {
    /// The shell completes file names itself.
    pub files: bool,
    /// The candidates (empty with `files`).
    pub candidates: Vec<Candidate>,
    /// The quote (`'` or `"`) left open in the word being completed: the answer's header carries
    /// it so the script escapes the candidates for that context (bash).
    pub open_quote: Option<char>,
}

impl Answer {
    /// The wire form (see the module docs).
    pub fn render(&self) -> String {
        if self.files {
            return "files\n".to_string();
        }
        let mut out = String::from("candidates");
        if let Some(q) = self.open_quote {
            out.push('\t');
            out.push(q);
        }
        out.push('\n');
        for c in &self.candidates {
            out.push_str(&clean(&c.text));
            if let Some(d) = &c.description {
                out.push('\t');
                out.push_str(&clean(d));
            }
            out.push('\n');
        }
        out
    }
}

/// A line-safe text (no tab or newline).
fn clean(s: &str) -> String {
    s.chars()
        .map(|c| {
            if matches!(c, '\n' | '\r' | '\t') {
                ' '
            } else {
                c
            }
        })
        .collect()
}

/// The command line a shell handed over, split into words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLine {
    /// The words before the one being completed, program name first.
    pub words: Vec<String>,
    /// The (unquoted) part of the word being completed that is left of the cursor: empty after a
    /// space.
    pub current: String,
    /// The quote still open at the end of the line (inside the word being completed).
    pub open_quote: Option<char>,
}

/// Split `line` the way a POSIX shell does (`'...'`, `"..."`, backslash escapes), with the last
/// word as [`CommandLine::current`]. An unterminated quote belongs to the current word.
/// `backslash_escapes` is false for PowerShell, where `\` is a path separator.
pub fn split_line(line: &str, backslash_escapes: bool) -> CommandLine {
    #[derive(Clone, Copy, PartialEq)]
    enum Quote {
        None,
        Single,
        Double,
    }
    let mut words: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut in_word = false;
    let mut quote = Quote::None;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Quote::Single => {
                if c == '\'' {
                    quote = Quote::None;
                } else {
                    buf.push(c);
                }
            }
            Quote::Double => match c {
                '"' => quote = Quote::None,
                '\\' if backslash_escapes => match chars.peek().copied() {
                    Some(next @ ('"' | '\\' | '$' | '`')) => {
                        buf.push(next);
                        chars.next();
                    }
                    _ => buf.push('\\'),
                },
                _ => buf.push(c),
            },
            Quote::None => match c {
                c if c.is_whitespace() => {
                    if in_word {
                        words.push(std::mem::take(&mut buf));
                        in_word = false;
                    }
                }
                '\'' => {
                    quote = Quote::Single;
                    in_word = true;
                }
                '"' => {
                    quote = Quote::Double;
                    in_word = true;
                }
                '\\' if backslash_escapes => {
                    in_word = true;
                    if let Some(next) = chars.next() {
                        buf.push(next);
                    }
                }
                _ => {
                    buf.push(c);
                    in_word = true;
                }
            },
        }
    }
    let current = if in_word || quote != Quote::None {
        buf
    } else {
        String::new()
    };
    let open_quote = match quote {
        Quote::None => None,
        Quote::Single => Some('\''),
        Quote::Double => Some('"'),
    };
    CommandLine {
        words,
        current,
        open_quote,
    }
}

/// The words of an [`ENV_WORDS`] value: the last is the current word.
pub fn split_words(joined: &str) -> CommandLine {
    let mut words: Vec<String> = joined.split(WORD_SEPARATOR).map(str::to_string).collect();
    let current = words.pop().unwrap_or_default();
    CommandLine {
        words,
        current,
        open_quote: None,
    }
}

/// The request in a process environment, if there is one.
pub fn request_from_env(var: impl Fn(&str) -> Option<String>) -> Option<(Shell, CommandLine)> {
    let shell = Shell::parse(&var(ENV_SHELL)?)?;
    let line = if let Some(words) = var(ENV_WORDS) {
        split_words(&words)
    } else {
        split_line(&var(ENV_LINE)?, shell != Shell::PowerShell)
    };
    Some((shell, line))
}

/// Answer a completion request if the environment holds one (the first thing [`crate::run`]
/// does): print the answer and return the exit code, or `None` for a normal run.
pub fn answer_if_requested() -> Option<i32> {
    let (_, line) = request_from_env(|k| std::env::var(k).ok())?;
    let vars = crate::dispatch::process_vars();
    let data = load_data(&vars);
    let answer = complete(&line, &data, &DateContext::system());
    let mut out = std::io::stdout().lock();
    // A closed pipe (the shell gave up) is not an error worth reporting.
    let _ = out.write_all(answer.render().as_bytes());
    let _ = out.flush();
    Some(0)
}

/// Everything the candidates read, from the environment and the root: connectors and config keys
/// (the registry with writes off), plugin names, jobs, and the plugin manifests as cached.
///
/// A root that cannot be resolved (a debug build with the default root, a missing
/// `MRSM_ROOT_DIR`) is not an error here: the candidates that need it are left out.
pub fn load_data(vars: &[(String, String)]) -> CompletionData {
    let mut data = CompletionData {
        actions: CORE_ACTIONS,
        do_names: cli_do_names(CORE_ACTIONS),
        include_v5_only: true,
        ..CompletionData::default()
    };
    // Reading the paths creates a missing `MRSM_VENVS_DIR` (as v4 does); completions never need
    // virtual environments and must not create anything.
    let vars: Vec<(String, String)> = vars
        .iter()
        .filter(|(k, _)| k != "MRSM_VENVS_DIR")
        .cloned()
        .collect();
    let mut options = meerschaum_core::config::ConfigOptions::with_env(vars);
    options.allow_writes = false;
    let Ok(config) = meerschaum_core::config::Config::new(options) else {
        return data;
    };
    data.connectors = connectors_from_config(config.get(&["meerschaum", "connectors"]).as_ref());
    data.config_keys = crate::builtin::possible_config_keys_of(&config);
    let paths = config.paths();
    data.plugin_names = crate::builtin::plugin_names(&paths.plugins_dirs);
    let manifest = cached_manifest(&paths.root_dir);
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
    data.instance_types = manifest
        .connector_types
        .values()
        .filter(|c| c.is_instance)
        .map(|c| c.connector_type.clone())
        .collect();
    data.executor_types = manifest.executor_types.keys().cloned().collect();
    let jobs_dir = meerschaum_jobs::JobPaths::from_config(&config).jobs_dir;
    data.jobs = Some(std::sync::Arc::new(move |executor_keys: &str| {
        if executor_keys != "local" {
            return Vec::new();
        }
        scan_jobs(&jobs_dir)
    }));
    data.instance_keys = config
        .get(&["meerschaum", "instance"])
        .map(|v| v.py_str())
        .unwrap_or_else(|| "sql:main".into());
    let config = std::sync::Arc::new(config);
    let runs_config = config.clone();
    data.runs = Some(std::sync::Arc::new(move |keys: &str| {
        let session = crate::native::Session::new();
        crate::native::engine_views::recent_run_ids(&runs_config, &session, keys, 20)
    }));
    data.pipelines = Some(std::sync::Arc::new(move |keys: &str| {
        let session = crate::native::Session::new();
        let Ok(instance) = session.instance(keys, &config) else {
            return Vec::new();
        };
        let query = meerschaum_connector::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        };
        let Ok(rows) = session.block_on(instance.conn().fetch_pipes_keys(&query)) else {
            return Vec::new();
        };
        let rows: Vec<_> = match rows {
            meerschaum_connector::PipesKeys::List(rows) => rows,
            meerschaum_connector::PipesKeys::Dict(rows) => rows.into_values().collect(),
        };
        let mut names: Vec<_> = rows
            .into_iter()
            .filter_map(|row| row.location_key)
            .collect();
        names.sort();
        names.dedup();
        names
    }));
    data
}

/// The local jobs, read-only: [`meerschaum_jobs::get_jobs`] and `Job::status` remove stale PID
/// files and rewrite lost ones, which a completion must never do. A job is listed when its
/// directory holds `properties.json`, unless it is hidden (`_` or `.` prefix) or externally
/// managed; its status comes from `process.pid` and the process state. A job whose PID file is
/// missing counts as stopped (the detached-process search of `Job::status` is not repeated).
pub fn scan_jobs(jobs_dir: &std::path::Path) -> Vec<crate::shell::completer::JobEntry> {
    use crate::shell::completer::JobEntry;
    use meerschaum_jobs::detached::Timestamps;
    use meerschaum_jobs::process::{ProcState, proc_state};
    let Ok(entries) = std::fs::read_dir(jobs_dir) else {
        return Vec::new();
    };
    let mut jobs: Vec<JobEntry> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let dir = entry.path();
        if name.starts_with(['_', '.'])
            || !dir.join("properties.json").is_file()
            || dir.join(".externally-managed").exists()
        {
            continue;
        }
        let props = meerschaum_jobs::properties::read_properties(&dir.join("properties.json"))
            .unwrap_or_default();
        let restart = props
            .get("restart")
            .is_some_and(meerschaum_core::value::Value::truthy);
        let pid_file = dir.join("process.pid");
        let pid = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|t| meerschaum_core::pyfmt::parse_py_int(t.trim_end()))
            .map(|p| p as i32);
        // A PID file older than the recorded end of the last run names no process of this job.
        let stale = {
            let timestamps = Timestamps::from_properties(&props);
            match (
                timestamps
                    .is_completed()
                    .then_some(timestamps.ended)
                    .flatten(),
                std::fs::metadata(&pid_file).and_then(|m| m.modified()),
            ) {
                (Some(ended), Ok(modified)) => {
                    chrono::DateTime::<chrono::Utc>::from(modified).naive_utc() <= ended
                }
                _ => false,
            }
        };
        let status = match pid.filter(|_| !stale).map(proc_state) {
            Some(ProcState::Alive) => "running",
            Some(ProcState::Stopped) => "paused",
            _ => "stopped",
        };
        jobs.push(JobEntry {
            name,
            status: status.to_string(),
            restart,
        });
    }
    jobs.sort_by(|a, b| a.name.cmp(&b.name));
    jobs
}

/// `mrsm show completions [shell]`, answered before the dispatcher (the first thing
/// [`crate::run`] does after a completion request) so that `source <(mrsm show completions bash)`
/// in a start-up file costs a few milliseconds and runs no config, plugin or worker code.
///
/// Only the plain forms are handled here (`show completions`, `show completion <shell>`); anything
/// else (`-h`, an unknown shell, other flags) goes to the normal action, which reports it.
pub fn print_script_if_requested(sysargs: &[String]) -> Option<i32> {
    let words: Vec<&str> = sysargs.iter().map(String::as_str).collect();
    let shell = match words.as_slice() {
        ["show", "completions" | "completion"] => {
            Shell::detect(std::env::var("SHELL").ok().as_deref())?
        }
        ["show", "completions" | "completion", name] if !name.starts_with('-') => {
            Shell::parse(name)?
        }
        _ => return None,
    };
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(install::script(shell).as_bytes());
    let _ = out.flush();
    Some(0)
}

/// The plugin manifests as the last command cached them (Python plugins, and native plugins on
/// top, as `Session::manifest` merges them), without validating or rebuilding either: a plugin
/// that changed since appears after the next command that loads plugins (`reload` forces it).
pub(crate) fn cached_manifest(root: &std::path::Path) -> meerschaum_worker::manifest::Manifest {
    use meerschaum_worker::manifest::{Manifest, manifest_path};
    let mut manifest = Manifest::read(&manifest_path(root))
        .ok()
        .flatten()
        .unwrap_or_default();
    let native = std::fs::read(crate::native_plugins::cache_path(root))
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
    if let Some(mut raw) = native {
        // The native manifest may omit argparse fields the worker manifest always lists.
        if let Some(plugins) = raw["plugins"].as_object_mut() {
            for plugin in plugins.values_mut() {
                for argument in plugin
                    .get_mut("arguments")
                    .and_then(serde_json::Value::as_array_mut)
                    .into_iter()
                    .flatten()
                {
                    for key in ["nargs", "const", "default", "choices", "metavar"] {
                        if argument.get(key).is_none() {
                            argument[key] = serde_json::Value::Null;
                        }
                    }
                }
            }
        }
        if let Ok(parsed) = serde_json::from_value::<Manifest>(raw) {
            manifest.plugins.extend(parsed.plugins);
            manifest.connector_types.extend(parsed.connector_types);
            manifest.executor_types.extend(parsed.executor_types);
        }
    }
    manifest
}

/// How many values a flag takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arity {
    /// A switch.
    Zero,
    /// One value.
    One,
    /// Values until the next flag (`nargs='+'` and `'*'`).
    Many,
    /// Everything after it (`-A` / `--sub-args`).
    Rest,
}

/// A flag the line may hold.
#[derive(Debug, Clone)]
struct Flag {
    dest: String,
    arity: Arity,
    choices: Vec<String>,
    path: bool,
    help: String,
}

/// Every flag by option string: the CLI spec, then the plugins' `add_plugin_argument`s.
fn flag_table(data: &CompletionData) -> IndexMap<String, Flag> {
    let mut table: IndexMap<String, Flag> = IndexMap::new();
    for arg in spec::ARGUMENTS.iter().filter(|a| !a.is_positional()) {
        let arity = match (arg.action, arg.nargs) {
            (spec::Action::StoreTrue, _) | (_, Nargs::Exactly(0)) => Arity::Zero,
            (_, Nargs::Single) | (_, Nargs::Exactly(1)) => Arity::One,
            (_, Nargs::Remainder) => Arity::Rest,
            _ => Arity::Many,
        };
        let flag = Flag {
            dest: arg.dest.to_string(),
            arity,
            choices: arg
                .choices
                .map(|c| c.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
            path: arg.arg_type == ArgType::Path,
            help: arg.help.to_string(),
        };
        for option in arg.option_strings {
            table.insert((*option).to_string(), flag.clone());
        }
    }
    for arg in &data.plugin_arguments {
        let switch = ["StoreTrue", "StoreFalse", "StoreConst", "Count", "Help"]
            .iter()
            .any(|k| arg.action.contains(k));
        let arity = if switch {
            Arity::Zero
        } else {
            match &arg.nargs {
                serde_json::Value::String(s) if s == "*" || s == "+" => Arity::Many,
                serde_json::Value::String(s) if s == "..." => Arity::Rest,
                serde_json::Value::Number(n) if n.as_u64() == Some(0) => Arity::Zero,
                serde_json::Value::Number(n) if n.as_u64().is_some_and(|n| n > 1) => Arity::Many,
                _ => Arity::One,
            }
        };
        let flag = Flag {
            dest: arg.dest.clone().unwrap_or_default(),
            arity,
            choices: arg
                .choices
                .as_array()
                .map(|c| {
                    c.iter()
                        .map(|v| {
                            v.as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| v.to_string())
                        })
                        .collect()
                })
                .unwrap_or_default(),
            path: arg.type_name.as_deref() == Some("Path"),
            help: arg.help.clone().unwrap_or_default(),
        };
        for option in &arg.option_strings {
            table.entry(option.clone()).or_insert_with(|| flag.clone());
        }
    }
    table
}

/// The table with the access flags of the command being typed (`edit pipes access`, `lock
/// pipes`, `register teams`, ...): only their own commands parse them
/// ([`crate::parse::access_command`]), so other commands never offer them.
fn with_access_flags(
    mut table: IndexMap<String, Flag>,
    words: &[String],
) -> IndexMap<String, Flag> {
    let Some(command) = crate::parse::access_command(words) else {
        return table;
    };
    for option in crate::parse::ENGINE_OPTIONS
        .iter()
        .filter(|o| crate::parse::access_flag_command(o.dest) == Some(command))
    {
        let help = crate::help::EXTRA_OPTIONS
            .iter()
            .find(|e| e.dest == option.dest)
            .map(|e| e.help)
            .unwrap_or_default();
        let choices: Vec<String> = match option.dest {
            "visibility" => crate::parse::VISIBILITY_CHOICES
                .iter()
                .map(|c| c.to_string())
                .collect(),
            "edit_mode" => crate::parse::EDIT_MODE_CHOICES
                .iter()
                .map(|c| c.to_string())
                .collect(),
            "locks" => meerschaum_core::access::Lock::KINDS
                .iter()
                .map(|l| l.name().to_string())
                .chain(std::iter::once("all".to_string()))
                .collect(),
            _ => Vec::new(),
        };
        table.insert(
            option.flag.to_string(),
            Flag {
                dest: option.dest.to_string(),
                arity: Arity::One,
                choices,
                path: false,
                help: help.to_string(),
            },
        );
    }
    table
}

/// A one-line description of a flag: the first sentence of its help, shortened.
fn describe(help: &str) -> Option<String> {
    let one_line = help.split_whitespace().collect::<Vec<_>>().join(" ");
    let first = one_line
        .split_once(". ")
        .map(|(a, _)| a.to_string())
        .unwrap_or_else(|| one_line.trim_end_matches('.').to_string());
    if first.is_empty() {
        return None;
    }
    const MAX: usize = 72;
    if first.chars().count() > MAX {
        let cut: String = first.chars().take(MAX - 3).collect();
        return Some(format!("{}...", cut.trim_end()));
    }
    Some(first)
}

/// What is known of the line before the current word.
#[derive(Debug, Default)]
struct Walk {
    /// The words of the current action (after the last `+`) that are not flags or their values.
    positional: Vec<String>,
    /// The flag whose values the current word continues, with the option string typed.
    pending: Option<(String, Flag)>,
    /// `-e` / `--executor-keys`, when given.
    executor_keys: Option<String>,
    /// A `-A` was seen: the rest of the line is not ours.
    rest: bool,
}

/// Classify the completed words (the program name is not among them).
fn walk(words: &[String], flags: &IndexMap<String, Flag>) -> Walk {
    let mut out = Walk::default();
    for word in words {
        if word == "+" {
            // The next action of a `+` chain starts over.
            out.positional.clear();
            out.pending = None;
            continue;
        }
        if word.starts_with('-') && word.len() > 1 {
            let (name, inline) = match word.split_once('=') {
                Some((n, v)) if word.starts_with("--") => (n, Some(v)),
                _ => (word.as_str(), None),
            };
            let Some(flag) = flags.get(name) else {
                out.pending = None;
                continue;
            };
            if flag.dest == "executor_keys"
                && let Some(v) = inline
            {
                out.executor_keys = Some(v.to_string());
            }
            out.pending = match flag.arity {
                Arity::Zero => None,
                Arity::One if inline.is_some() => None,
                Arity::Many | Arity::One => Some((name.to_string(), flag.clone())),
                Arity::Rest => {
                    out.rest = true;
                    return out;
                }
            };
            continue;
        }
        match out.pending.take() {
            Some((name, flag)) => {
                if flag.dest == "executor_keys" {
                    out.executor_keys = Some(word.clone());
                }
                if flag.arity == Arity::Many {
                    out.pending = Some((name, flag));
                }
            }
            None => out.positional.push(word.clone()),
        }
    }
    out
}

/// The candidates for `line` (the program name is its first word).
pub fn complete(line: &CommandLine, data: &CompletionData, dates: &DateContext) -> Answer {
    let mut answer = complete_words(line, data, dates);
    answer.open_quote = line.open_quote;
    answer
}

fn complete_words(line: &CommandLine, data: &CompletionData, dates: &DateContext) -> Answer {
    let args: &[String] = line.words.get(1..).unwrap_or(&[]);
    let current = line.current.as_str();
    // The program name alone is being typed: nothing to complete.
    if line.words.is_empty() {
        return Answer::default();
    }
    let flags = with_access_flags(flag_table(data), args);
    let walked = walk(args, &flags);
    if walked.rest {
        return Answer::default();
    }

    // `--flag=value`.
    if current.starts_with("--")
        && let Some((name, value)) = current.split_once('=')
        && let Some(flag) = flags.get(name)
    {
        let mut answer = flag_values(flag, value, data);
        for c in &mut answer.candidates {
            c.text = format!("{name}={}", c.text);
        }
        return answer;
    }
    if current.starts_with('-') {
        return flag_names(current, &flags);
    }
    if let Some((_, flag)) = &walked.pending {
        return flag_values(flag, current, data);
    }
    positional(&walked, current, data, dates)
}

/// The option strings that start with `prefix`, with a description each.
fn flag_names(prefix: &str, flags: &IndexMap<String, Flag>) -> Answer {
    let mut names: BTreeSet<&str> = BTreeSet::new();
    names.extend(
        flags
            .keys()
            .map(String::as_str)
            .filter(|n| n.starts_with(prefix)),
    );
    Answer {
        files: false,
        open_quote: None,
        candidates: names
            .into_iter()
            .map(|n| Candidate {
                text: n.to_string(),
                description: describe(&flags[n].help),
            })
            .collect(),
    }
}

/// Candidates for the value of `flag` that starts with `prefix`.
fn flag_values(flag: &Flag, prefix: &str, data: &CompletionData) -> Answer {
    if flag.path {
        return Answer {
            files: true,
            open_quote: None,
            candidates: Vec::new(),
        };
    }
    let names: Vec<String> = if !flag.choices.is_empty() {
        flag.choices
            .iter()
            .filter(|c| c.starts_with(prefix))
            .cloned()
            .collect()
    } else {
        match flag.dest.as_str() {
            "connector_keys" => data.connector_labels(&[], prefix, true, &[]),
            "mrsm_instance" => {
                let mut types: Vec<&str> = INSTANCE_TYPES.to_vec();
                types.extend(data.instance_types.iter().map(String::as_str));
                data.connector_labels(&types, prefix, true, &[])
            }
            "repository" => data.connector_labels(&["api"], prefix, true, &[]),
            "executor_keys" => data.executor_candidates(prefix, &["local", "systemd"]),
            _ => Vec::new(),
        }
    };
    Answer {
        files: false,
        open_quote: None,
        candidates: names.into_iter().map(Candidate::plain).collect(),
    }
}

/// Candidates for a positional word: the interactive shell's, over the words of the current
/// action without its flags.
fn positional(walked: &Walk, current: &str, data: &CompletionData, dates: &DateContext) -> Answer {
    let mut data = data.clone();
    if let Some(executor) = &walked.executor_keys {
        data.executor_keys = executor.clone();
    }
    // The words before the current one, as the shell prompt would hold them.
    let before = if walked.positional.is_empty() {
        String::new()
    } else {
        format!("{} ", walked.positional.join(" "))
    };
    if before.is_empty() && current.is_empty() {
        return Answer {
            files: false,
            open_quote: None,
            candidates: data
                .do_names
                .iter()
                .map(|n| Candidate::plain(n.clone()))
                .collect(),
        };
    }
    let text = format!("{before}{current}");
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut candidates = Vec::new();
    for completion in data.get_completions(&text, dates) {
        let Some(mut word) = replacement(&text, &before, &completion) else {
            continue;
        };
        // A plugin action `foo_bar` is one program argument when it is the first word.
        if before.is_empty() {
            word = word.replace(' ', "_");
        }
        if !word.is_empty() && word.starts_with(current) && seen.insert(word.clone()) {
            candidates.push(Candidate::plain(word));
        }
    }
    Answer {
        files: false,
        open_quote: None,
        candidates,
    }
}

/// What `completion` makes of the word being typed: apply it to `text`, then cut off the words
/// before (`before`, ending in a space or empty).
fn replacement(text: &str, before: &str, completion: &Completion) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let keep = chars
        .len()
        .saturating_sub(usize::try_from(-completion.start_position).ok()?);
    let applied: String = chars[..keep]
        .iter()
        .copied()
        .chain(completion.text.chars())
        .collect();
    applied.strip_prefix(before).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> CompletionData {
        let mut connectors = IndexMap::new();
        connectors.insert("sql".to_string(), vec!["main".into(), "local".into()]);
        connectors.insert("api".to_string(), vec!["mrsm".into()]);
        CompletionData {
            actions: CORE_ACTIONS,
            do_names: cli_do_names(CORE_ACTIONS),
            include_v5_only: true,
            connectors,
            ..CompletionData::default()
        }
    }

    fn words(line: &str) -> CommandLine {
        split_line(line, true)
    }

    fn texts(answer: &Answer) -> Vec<&str> {
        answer.candidates.iter().map(|c| c.text.as_str()).collect()
    }

    fn run(line: &str) -> Answer {
        complete(&words(line), &data(), &DateContext::system())
    }

    #[test]
    fn splits_quotes_escapes_and_the_current_word() {
        let l = words("mrsm show pipes ");
        assert_eq!(l.words, ["mrsm", "show", "pipes"]);
        assert_eq!(l.current, "");
        let l = words("mrsm  sh");
        assert_eq!(l.words, ["mrsm"]);
        assert_eq!(l.current, "sh");
        let l = words(r#"mrsm -l 'a b' "c \"d\" e" f\ g h"#);
        assert_eq!(l.words, ["mrsm", "-l", "a b", r#"c "d" e"#, "f g"]);
        assert_eq!(l.current, "h");
        // An unterminated quote is the current word.
        let l = words("mrsm -c \"sql:ma");
        assert_eq!(l.words, ["mrsm", "-c"]);
        assert_eq!(l.current, "sql:ma");
        let l = words("mrsm -m '");
        assert_eq!(l.current, "");
        assert_eq!(l.words, ["mrsm", "-m"]);
        // Adjacent quoted and bare parts are one word.
        assert_eq!(words("mrsm a'b c'd ").words, ["mrsm", "ab cd"]);
        // PowerShell paths keep their backslashes.
        assert_eq!(split_line(r"mrsm --file C:\a\b", false).current, r"C:\a\b");
        assert_eq!(split_line("", true).words, Vec::<String>::new());
    }

    #[test]
    fn splits_the_words_protocol() {
        let l = split_words("mrsm\u{1f}show\u{1f}");
        assert_eq!(l.words, ["mrsm", "show"]);
        assert_eq!(l.current, "");
        let l = split_words("mrsm\u{1f}sh");
        assert_eq!(l.words, ["mrsm"]);
        assert_eq!(l.current, "sh");
        assert_eq!(split_words("").current, "");
    }

    #[test]
    fn requests_need_a_shell_and_a_line() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(name, _)| *name == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert!(request_from_env(env(&[])).is_none());
        assert!(request_from_env(env(&[("_MRSM_COMPLETE", "bash")])).is_none());
        assert!(request_from_env(env(&[("_MRSM_COMPLETE_LINE", "mrsm ")])).is_none());
        assert!(
            request_from_env(env(&[
                ("_MRSM_COMPLETE", "nushell"),
                ("_MRSM_COMPLETE_LINE", "mrsm ")
            ]))
            .is_none()
        );
        let (shell, line) = request_from_env(env(&[
            ("_MRSM_COMPLETE", "pwsh"),
            ("_MRSM_COMPLETE_LINE", r"mrsm --file C:\x"),
        ]))
        .unwrap();
        assert_eq!(shell, Shell::PowerShell);
        assert_eq!(line.current, r"C:\x");
    }

    #[test]
    fn shells_are_detected_from_the_environment() {
        assert_eq!(Shell::detect(Some("/usr/bin/zsh")), Some(Shell::Zsh));
        assert_eq!(Shell::detect(Some("/bin/bash")), Some(Shell::Bash));
        assert_eq!(
            Shell::detect(Some("/usr/local/bin/fish")),
            Some(Shell::Fish)
        );
        assert_eq!(
            Shell::detect(Some(r"C:\Program Files\PowerShell\7\pwsh.exe")),
            Some(Shell::PowerShell)
        );
        assert_eq!(Shell::detect(Some("/bin/dash")), None);
        assert_eq!(Shell::detect(None).is_some(), cfg!(windows));
    }

    #[test]
    fn actions_complete_with_the_v5_ones_and_without_shell_commands() {
        let a = run("mrsm sh");
        assert_eq!(texts(&a), ["sh", "show"]);
        let a = run("mrsm pl");
        assert_eq!(texts(&a), ["plan"]);
        let all = run("mrsm ");
        assert!(texts(&all).contains(&"prune"));
        assert!(texts(&all).contains(&"show"));
        // `instance` and `exit` are shell commands, not program actions; `help` is both (rc15:
        // `mrsm help`).
        for shell_only in ["instance", "exit", "pass", "debug"] {
            assert!(!texts(&all).contains(&shell_only), "{shell_only}");
        }
        assert!(texts(&all).contains(&"help"));
        assert!(run("mrsm zzz").candidates.is_empty());
    }

    #[test]
    fn subactions_include_the_v5_only_ones() {
        let a = run("mrsm show ");
        let t = texts(&a);
        assert!(t.contains(&"pipes") && t.contains(&"graph") && t.contains(&"drivers"));
        assert_eq!(texts(&run("mrsm show gr")), ["graph"]);
        let a = run("mrsm install ");
        assert!(texts(&a).contains(&"drivers"));
    }

    #[test]
    fn flags_complete_anywhere_with_descriptions() {
        let a = run("mrsm show pipes -");
        let t = texts(&a);
        assert!(t.contains(&"-c") && t.contains(&"--connector-keys") && t.contains(&"--yes"));
        let a = run("mrsm show pipes --conn");
        assert_eq!(texts(&a), ["--connector-keys"]);
        assert!(
            a.candidates[0]
                .description
                .as_deref()
                .unwrap()
                .contains("connector keys")
        );
        // Before the action, between words, after values.
        assert_eq!(texts(&run("mrsm --insta")), ["--instance"]);
        assert_eq!(
            texts(&run("mrsm sync pipes -i sql:main --noas")),
            ["--noask"]
        );
    }

    #[test]
    fn flag_values_are_connector_labels() {
        let a = run("mrsm show pipes -c ");
        assert_eq!(texts(&a), ["api:mrsm", "sql:local", "sql:main"]);
        assert_eq!(texts(&run("mrsm show pipes -c sql:ma")), ["sql:main"]);
        assert_eq!(texts(&run("mrsm -i s")), ["sql:local", "sql:main"]);
        assert_eq!(
            texts(&run("mrsm show pipes --instance ")),
            ["api:mrsm", "sql:local", "sql:main"]
        );
        assert_eq!(texts(&run("mrsm -r ")), ["api:mrsm"]);
        assert_eq!(
            texts(&run("mrsm show jobs -e ")),
            ["api:mrsm", "local", "systemd"]
        );
        // A list flag keeps taking values until the next flag.
        assert_eq!(
            texts(&run("mrsm show pipes -c sql:main s")),
            ["sql:local", "sql:main"]
        );
        // `--flag=value` completes the value and keeps the flag.
        assert_eq!(
            texts(&run("mrsm show pipes --instance=sql:l")),
            ["--instance=sql:local"]
        );
    }

    #[test]
    fn access_flags_complete_only_for_their_commands() {
        assert_eq!(
            texts(&run("mrsm edit pipes access --vis")),
            ["--visibility"]
        );
        assert_eq!(
            texts(&run("mrsm edit pipes access -m x --visibility ")),
            ["team", "private", "everyone"]
        );
        assert_eq!(texts(&run("mrsm edit pipes access --edit o")), ["owner"]);
        assert_eq!(texts(&run("mrsm lock pipes --locks d")), ["drop", "delete"]);
        assert_eq!(texts(&run("mrsm register teams a --mem")), ["--members"]);
        // A flag value is not an action word: `--team x` leaves the selection flags to complete.
        assert!(texts(&run("mrsm edit pipes access --team x -")).contains(&"--owner"));
        // Other commands never offer them.
        assert!(run("mrsm sync pipes --vis").candidates.is_empty());
        assert!(run("mrsm edit pipes --own").candidates.is_empty());
        assert!(run("mrsm show teams --mem").candidates.is_empty());
    }

    #[test]
    fn flags_without_candidates_say_nothing_and_paths_ask_for_files() {
        assert!(run("mrsm show pipes -m ").candidates.is_empty());
        assert!(!run("mrsm show pipes -m ").files);
        assert!(run("mrsm compose --file ").files);
        assert!(run("mrsm compose --env-file ").files);
        assert!(run("mrsm compose --file=").files);
        // `-A` hands the rest of the line to a subprocess.
        let a = run("mrsm stack -A ");
        assert!(a.candidates.is_empty() && !a.files);
    }

    #[test]
    fn flag_values_do_not_count_as_action_words() {
        // `-i sql:main` and the switch are skipped: both `pi` subactions remain.
        assert_eq!(
            texts(&run("mrsm show -i sql:main --yes pi")),
            ["pipelines", "pipes"]
        );
        // A `+` starts the next action.
        assert_eq!(texts(&run("mrsm sync pipes + sh")), ["sh", "show"]);
        // An executor given earlier selects the job list.
        assert!(run("mrsm show jobs -e api:mrsm ").candidates.is_empty());
    }

    #[test]
    fn plugin_actions_and_underscored_names() {
        use meerschaum_worker::manifest::{ActionManifest, SubactionManifest};
        let mut d = data();
        d.plugin_actions = vec![
            ActionManifest {
                name: "hello".into(),
                subactions: vec![SubactionManifest {
                    name: "world".into(),
                    doc: None,
                }],
                ..ActionManifest::default()
            },
            ActionManifest {
                name: "foo_bar".into(),
                ..ActionManifest::default()
            },
        ];
        d.do_names
            .extend(["hello".to_string(), "foo_bar".to_string()]);
        d.do_names.sort();
        let dates = DateContext::system();
        let a = complete(&words("mrsm hel"), &d, &dates);
        assert_eq!(texts(&a), ["hello", "help"]);
        let a = complete(&words("mrsm hell"), &d, &dates);
        assert_eq!(texts(&a), ["hello"]);
        let a = complete(&words("mrsm hello w"), &d, &dates);
        assert_eq!(texts(&a), ["world"]);
        // The first word keeps its underscore; after the first part a space-separated word works.
        let a = complete(&words("mrsm foo"), &d, &dates);
        assert_eq!(texts(&a), ["foo_bar"]);
        let a = complete(&words("mrsm foo "), &d, &dates);
        assert_eq!(texts(&a), ["bar"]);
    }

    #[test]
    fn plugin_subcommands_complete_under_their_first_word() {
        use meerschaum_worker::manifest::ActionManifest;
        let mut d = data();
        // `weather_wait` and `weather_status` (no `weather` action of their own) run as
        // `mrsm weather wait`.
        d.plugin_actions = ["weather_wait", "weather_status"]
            .iter()
            .map(|name| ActionManifest {
                name: name.to_string(),
                ..ActionManifest::default()
            })
            .collect();
        d.do_names
            .extend(["weather_wait".to_string(), "weather_status".to_string()]);
        d.do_names.sort();
        let dates = DateContext::system();
        let mut subcommands = texts(&complete(&words("mrsm weather "), &d, &dates))
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        subcommands.sort();
        assert_eq!(subcommands, ["status", "wait"]);
        assert_eq!(
            texts(&complete(&words("mrsm weather w"), &d, &dates)),
            ["wait"]
        );
    }

    /// `@make_action(name='weather wait_for')`: the explicit words keep their underscore in
    /// completion (an underscore name would split into `wait for`).
    #[test]
    fn explicit_action_words_keep_their_underscores() {
        use meerschaum_worker::manifest::ActionManifest;
        let mut d = data();
        d.plugin_actions = vec![ActionManifest {
            name: "weather_wait_for".into(),
            words: vec!["weather".into(), "wait_for".into()],
            ..ActionManifest::default()
        }];
        d.do_names.push("weather_wait_for".to_string());
        d.do_names.sort();
        let dates = DateContext::system();
        assert_eq!(
            texts(&complete(&words("mrsm weather "), &d, &dates)),
            ["wait_for"]
        );
        assert_eq!(
            texts(&complete(&words("mrsm weather wait_"), &d, &dates)),
            ["wait_for"]
        );
        let shell: Vec<String> = d
            .get_completions("weather wa", &dates)
            .into_iter()
            .map(|c| c.text)
            .collect();
        assert_eq!(shell, ["weather wait_for"]);
    }

    #[test]
    fn plugin_arguments_are_flags() {
        use meerschaum_worker::manifest::ArgumentManifest;
        let mut d = data();
        d.plugin_arguments = vec![
            ArgumentManifest {
                option_strings: vec!["--color".into()],
                dest: Some("color".into()),
                action: "_StoreAction".into(),
                choices: serde_json::json!(["red", "blue"]),
                help: Some("Pick a color. Really.".into()),
                ..ArgumentManifest::default()
            },
            ArgumentManifest {
                option_strings: vec!["--shout".into()],
                dest: Some("shout".into()),
                action: "_StoreTrueAction".into(),
                ..ArgumentManifest::default()
            },
        ];
        let dates = DateContext::system();
        let a = complete(&words("mrsm show pipes --col"), &d, &dates);
        assert_eq!(texts(&a), ["--color"]);
        assert_eq!(a.candidates[0].description.as_deref(), Some("Pick a color"));
        assert_eq!(
            texts(&complete(&words("mrsm show pipes --color "), &d, &dates)),
            ["red", "blue"]
        );
        // A switch takes no value: the next word is positional again.
        assert_eq!(
            texts(&complete(&words("mrsm show --shout pi"), &d, &dates)),
            ["pipelines", "pipes"]
        );
    }

    #[test]
    fn answers_render_for_the_scripts() {
        let a = Answer {
            files: false,
            open_quote: None,
            candidates: vec![
                Candidate::plain("show"),
                Candidate {
                    text: "--yes".into(),
                    description: Some("Agree\tto it".into()),
                },
            ],
        };
        assert_eq!(a.render(), "candidates\nshow\n--yes\tAgree to it\n");
        assert_eq!(Answer::default().render(), "candidates\n");
        assert_eq!(
            Answer {
                files: true,
                open_quote: None,
                candidates: Vec::new()
            }
            .render(),
            "files\n"
        );
    }

    #[test]
    fn descriptions_are_one_short_sentence() {
        assert_eq!(
            describe("Agree to the default choices. More text."),
            Some("Agree to the default choices".into())
        );
        assert_eq!(describe(""), None);
        let long = "word ".repeat(40);
        assert!(describe(&long).unwrap().chars().count() <= 72);
    }
}
