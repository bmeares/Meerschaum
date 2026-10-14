//! The `mrsm` entry point and action dispatch, ported from `meerschaum/__main__.py` (`main`),
//! `meerschaum/_internal/entry.py` (`entry_without_daemon`, `entry_with_args`) and
//! `meerschaum/_internal/arguments/_parser.py` (`parse_help`, `parse_version`).
//!
//! Core actions resolve by the v4 naming convention and use native implementations or the
//! shared Python worker for compatibility operations.

use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use meerschaum_core::config::{Config, ConfigOptions};

use crate::actions::{self, CORE_ACTIONS, Resolved};
use crate::builtin::{self, ActionError, ActionResult};
use crate::env;
use crate::format::{FormatSettings, Formatter, OptionsArgs, TupleArgs};
use crate::help;
use crate::native;
use crate::parse::parse_arguments;
use crate::schema;
use crate::shlex;
use crate::spec::keys;
use crate::sysargs::{
    get_pipeline_sysargs, split_chained_sysargs, split_pipeline_sysargs,
    sysargs_has_api_executor_keys,
};
use crate::types::DateContext;
use crate::value::{ArgsMap, PyException, PyValue};

pub use meerschaum_core::SuccessTuple;

/// The flag that prints the argument JSON schema (Rust-only; intercepted before parsing).
pub const PRINT_ARG_SCHEMA_FLAG: &str = "--print-arg-schema";

/// v4 recurses through `entry` when a `+` survives into `entry_with_args`; cap the depth.
const MAX_ENTRY_DEPTH: usize = 32;

/// A process the CLI starts (an editor, `bash`, `python`, `mrsm` for compose).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandSpec {
    /// The program.
    pub program: String,
    /// Its arguments.
    pub args: Vec<String>,
    /// Environment variables to set.
    pub env: Vec<(String, String)>,
    /// Start from an empty environment (`env` is the whole environment).
    pub env_clear: bool,
    /// Discard the output (`capture_output=True`).
    pub quiet: bool,
    /// The working directory.
    pub cwd: Option<PathBuf>,
}

/// The side effects actions need from the host: running processes in the foreground and
/// reading a line of input.
pub trait Host {
    /// Run a process with the terminal attached and return its exit code.
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32>;

    /// Print a prompt and read a line from stdin (`None` at end of input).
    fn read_line(&mut self, prompt: &str) -> Option<String>;
}

/// The real host.
#[derive(Debug, Default)]
pub struct SystemHost;

impl Host for SystemHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        let mut cmd = std::process::Command::new(&spec.program);
        cmd.args(&spec.args);
        if spec.env_clear {
            cmd.env_clear();
        }
        cmd.envs(spec.env.iter().map(|(k, v)| (k, v)));
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        if spec.quiet {
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
        }
        let status = cmd.status()?;
        Ok(status.code().unwrap_or(1))
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        print!("{prompt}");
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line.trim_end_matches(['\n', '\r']).to_string()),
        }
    }
}

/// A host that runs nothing: every process "exits" with a fixed code and input is closed.
/// The commands are recorded (for tests).
#[derive(Debug, Default)]
pub struct RecordingHost {
    /// The processes that would have run.
    pub commands: Vec<CommandSpec>,
    /// The exit code every process returns.
    pub exit_code: i32,
    /// Lines to answer prompts with.
    pub input: Vec<String>,
}

impl Host for RecordingHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        self.commands.push(spec.clone());
        Ok(self.exit_code)
    }

    fn read_line(&mut self, _prompt: &str) -> Option<String> {
        if self.input.is_empty() {
            None
        } else {
            Some(self.input.remove(0))
        }
    }
}

/// The dispatcher's environment: clock, terminal width, output streams, process environment,
/// config registry and host.
pub struct Runtime<'a> {
    /// Clock inputs for datetime parsing.
    pub dates: DateContext,
    /// Help width in columns.
    pub columns: usize,
    /// Where actions print.
    pub out: &'a mut dyn Write,
    /// Where warnings go.
    pub err: Box<dyn Write + 'a>,
    /// Environment lookup (for `MRSM_*` variables).
    pub env: fn(&str) -> Option<String>,
    /// The whole environment, in process order (`os.environ`).
    pub vars: fn() -> Vec<(String, String)>,
    /// The config registry (built from the process environment on first use).
    pub config: Option<Config>,
    /// Formatting settings (detected from the config and terminal when `None`).
    pub format: Option<FormatSettings>,
    /// Processes and input.
    pub host: Box<dyn Host + 'a>,
    /// The working directory (the process's when `None`).
    pub cwd: Option<PathBuf>,
    /// Running inside the interactive shell.
    pub in_shell: bool,
    /// The connectors and Tokio runtime the native instance actions use (built on first use).
    pub native: Option<crate::native::Session>,
    /// Stdout is a terminal: graph runs print live progress lines before the Gantt chart. Only
    /// the process runtime sets it, so tests and piped output never see progress lines.
    pub live: bool,
}

fn no_vars() -> Vec<(String, String)> {
    Vec::new()
}

fn process_vars() -> Vec<(String, String)> {
    std::env::vars().collect()
}

impl<'a> Runtime<'a> {
    /// The process runtime writing to `out` (warnings to stderr).
    pub fn system(out: &'a mut dyn Write) -> Self {
        Self {
            dates: DateContext::system(),
            columns: help::terminal_columns(),
            out,
            err: Box::new(std::io::stderr()),
            env: env::process_env,
            vars: process_vars,
            config: None,
            format: None,
            host: Box::new(SystemHost),
            cwd: None,
            in_shell: false,
            native: None,
            live: std::io::IsTerminal::is_terminal(&std::io::stdout()),
        }
    }

    /// A runtime with an explicit environment lookup and no process environment, host or
    /// stderr (for tests); set `config` before running actions that read it.
    pub fn new(
        dates: DateContext,
        columns: usize,
        out: &'a mut dyn Write,
        env: fn(&str) -> Option<String>,
    ) -> Self {
        Self {
            dates,
            columns,
            out,
            err: Box::new(std::io::sink()),
            env,
            vars: no_vars,
            config: None,
            format: None,
            host: Box::new(RecordingHost::default()),
            cwd: None,
            in_shell: false,
            native: None,
            live: false,
        }
    }

    /// The config registry.
    pub fn config(&mut self) -> &Config {
        self.config.get_or_insert_with(|| {
            Config::new(ConfigOptions::from_process()).unwrap_or_else(|e| {
                if let meerschaum_core::config::ConfigError::DefaultRootRefused { .. } = e {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
                let mut opts = ConfigOptions::from_process();
                opts.allow_writes = false;
                Config::new(opts).expect("config registry")
            })
        })
    }

    /// The formatting settings.
    pub fn settings(&mut self) -> FormatSettings {
        if let Some(s) = &self.format {
            return s.clone();
        }
        let env = self.env;
        FormatSettings::detect(self.config(), env)
    }

    /// Run a closure with a [`Formatter`].
    pub fn with_formatter<R>(&mut self, f: impl FnOnce(&Formatter<'_>) -> R) -> R {
        let settings = self.settings();
        let config = self.config();
        let formatter = Formatter::new(settings, config);
        f(&formatter)
    }

    /// Write to stdout.
    pub fn print(&mut self, text: &str) {
        let _ = self.out.write_all(text.as_bytes());
        let _ = self.out.flush();
    }

    /// Write to stderr.
    pub fn eprint(&mut self, text: &str) {
        let _ = self.err.write_all(text.as_bytes());
        let _ = self.err.flush();
    }
}

fn contains(sysargs: &[String], flag: &str) -> bool {
    sysargs.iter().any(|a| a == flag)
}

/// `meerschaum.__main__.main`: returns the process exit code.
pub fn main(sysargs: &[String], rt: &mut Runtime<'_>) -> i32 {
    if let Err(e) = crate::python::manifest(rt) {
        rt.eprint(&format!("Unable to load plugin manifest: {}\n", e.message));
    }
    if contains(sysargs, PRINT_ARG_SCHEMA_FLAG) {
        let schema = serde_json::to_string_pretty(&schema::arg_schema()).unwrap_or_default();
        let _ = writeln!(rt.out, "{schema}");
        return 0;
    }
    if contains(sysargs, "--help") || contains(sysargs, "-h") {
        parse_help(sysargs, rt);
        return 0;
    }
    if contains(sysargs, "--version") || contains(sysargs, "-V") {
        parse_version(sysargs, rt);
        return 0;
    }
    let nopretty = contains(sysargs, "--nopretty");
    if sysargs.is_empty() || contains(sysargs, "--shell") {
        let sysargs: Vec<String> = sysargs
            .iter()
            .filter(|a| *a != "--shell")
            .cloned()
            .collect();
        // PARITY: `get_shell(sysargs)` without `mrsm_instance` (`meerschaum/__main__.py`).
        crate::shell::run(&sysargs, None, rt);
        return 0;
    }
    let result = entry(sysargs, rt);
    let stdio_mcp = parse_arguments(sysargs, &rt.dates)
        .map(|args| {
            crate::native::words(&args, "action").starts_with(&["start".into(), "mcp".into()])
        })
        .unwrap_or(false);
    if stdio_mcp {
        if !result.success {
            rt.eprint(&format!("{}\n", result.message));
        }
        return if result.success { 0 } else { 1 };
    }
    // A job's worker reports its result to the supervisor (`meerschaum-jobs`), which records it
    // in the job's properties like v4's daemon does.
    if let Some(path) = (rt.env)(meerschaum_jobs::supervisor::RESULT_PATH_ENV) {
        let json = serde_json::json!([result.success, result.message]).to_string();
        let _ = std::fs::write(path, json);
    }
    // PARITY: v4 only sets a failing exit code when the tuple is printed, so `--nopretty`
    // always exits 0 (`meerschaum/__main__.py` `main`).
    if nopretty {
        return 0;
    }
    let text = rt.with_formatter(|f| {
        f.print_tuple(
            &result,
            TupleArgs {
                upper_padding: 1,
                ..TupleArgs::default()
            },
        )
    });
    rt.print(&text);
    if result.success { 0 } else { 1 }
}

/// `entry(sysargs)`: run a (possibly chained) command line.
pub fn entry(sysargs: &[String], rt: &mut Runtime<'_>) -> SuccessTuple {
    entry_at_depth(sysargs, rt, 0)
}

fn entry_at_depth(sysargs: &[String], rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
    if depth > MAX_ENTRY_DEPTH {
        // PARITY: v4 recurses without bound here and dies with RecursionError
        // (`entry_with_args` re-entering `entry` for `start job x + ...`).
        return SuccessTuple::fail("RecursionError: maximum recursion depth exceeded");
    }
    let dates = rt.dates.clone();
    let result = run_chain(sysargs, &dates, None, |args| {
        entry_with_args(args, rt, depth)
    });
    match result {
        Ok(tup) => tup,
        Err(e) => SuccessTuple::fail(format!("{}: {}", e.class, e.message)),
    }
}

/// `entry(sysargs, _patch_args=patch)`: the shell's entry point (`shell=True`, `line=...`).
pub fn entry_with_patch(sysargs: &[String], patch: &ArgsMap, rt: &mut Runtime<'_>) -> SuccessTuple {
    let dates = rt.dates.clone();
    let result = run_chain(sysargs, &dates, Some(patch), |args| {
        entry_with_args(args, rt, 0)
    });
    match result {
        Ok(tup) => tup,
        Err(e) => SuccessTuple::fail(format!("{}: {}", e.class, e.message)),
    }
}

/// `entry_without_daemon` without the shell's executor keys and the systemd result file:
/// split `:` pipeline flags and `+` chains, parse each step and run `execute` until one fails.
pub fn run_chain<F>(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
    mut execute: F,
) -> Result<SuccessTuple, PyException>
where
    F: FnMut(ArgsMap) -> SuccessTuple,
{
    let (sysargs, pipeline_args) = split_pipeline_sysargs(sysargs);
    let has_daemon = contains(&sysargs, "-d") || contains(&sysargs, "--daemon");
    let has_start_job = sysargs.len() >= 2 && sysargs[0] == "start" && sysargs[1] == "job";
    let pipeline_has_api_executor_keys = sysargs_has_api_executor_keys(&pipeline_args);

    let mut chained = if has_daemon || has_start_job || pipeline_has_api_executor_keys {
        vec![sysargs.clone()]
    } else {
        split_chained_sysargs(&sysargs)?
    };
    if !pipeline_args.is_empty() {
        chained = vec![get_pipeline_sysargs(&sysargs, &pipeline_args, patch_args)];
    }

    let mut results: Vec<SuccessTuple> = Vec::new();
    let mut steps: Vec<(Instant, Instant)> = Vec::new();
    for step in &chained {
        let step: Vec<String> = step
            .iter()
            .map(|a| {
                if a == keys::ESCAPED_PIPELINE {
                    keys::PIPELINE.to_string()
                } else {
                    a.clone()
                }
            })
            .collect();
        let mut args = parse_arguments(&step, dates)?;
        if let Some(patch) = patch_args {
            for (k, v) in patch {
                args.insert(k.clone(), v.clone());
            }
        }
        if let Some(PyValue::Exception(e)) = args.get(keys::FAILURE_KEY) {
            let text = args.get("text").map(PyValue::py_str).unwrap_or_default();
            if !text.starts_with("show arguments") {
                let mut msg = "Invalid arguments:".to_string();
                if !text.is_empty() {
                    msg.push('\n');
                    msg.push_str(&text);
                }
                msg.push_str("\n    ");
                msg.push_str(&e.message);
                return Ok(SuccessTuple::fail(msg));
            }
        }
        let start = Instant::now();
        let result = execute(args);
        steps.push((start, Instant::now()));
        let ok = result.success;
        results.push(result);
        if !ok {
            break;
        }
    }

    if chained.len() <= 1 {
        return Ok(results
            .into_iter()
            .next()
            .unwrap_or_else(SuccessTuple::success));
    }
    Ok(summarize_chain(&chained, &results, &steps))
}

/// `make_header(message, left_pad, top=False)` with the Unicode ruler.
fn make_header(message: &str, left_pad: usize) -> String {
    let max_len = message
        .split('\n')
        .map(|w| w.chars().count())
        .max()
        .unwrap_or(0);
    let left = " ".repeat(left_pad);
    format!(
        "{left}{}\n{left}{}",
        message.replace('\n', &format!("\n{left}")),
        "─".repeat(max_len)
    )
}

fn seconds_str(secs: f64) -> String {
    format!("{:.2} seconds", secs)
}

/// The multi-step message of `entry_without_daemon` (durations formatted in seconds).
fn summarize_chain(
    chained: &[Vec<String>],
    results: &[SuccessTuple],
    steps: &[(Instant, Instant)],
) -> SuccessTuple {
    let success = results.iter().all(|r| r.success);
    let successes: Vec<&String> = results
        .iter()
        .filter(|r| r.success)
        .map(|r| &r.message)
        .collect();
    let total = match (steps.first(), steps.last()) {
        (Some(first), Some(last)) => Some(last.1.duration_since(first.0).as_secs_f64()),
        _ => None,
    };
    let plural = |n: usize| if n != 1 { "s" } else { "" };
    let mut msg = String::new();
    if success {
        msg.push_str("Successfully c");
    } else {
        msg.push_str(&format!(
            "Failed pipeline after {} step{}.\n\nC",
            successes.len(),
            plural(successes.len())
        ));
    }
    msg.push_str(&format!(
        "ompleted {} step{}",
        successes.len(),
        plural(successes.len())
    ));
    if let Some(t) = total {
        msg.push_str(&format!(" in {}", seconds_str(t)));
    }
    msg.push_str(":\n\n");
    let mut parts = Vec::new();
    for (i, (m, step)) in successes.iter().zip(chained).enumerate() {
        let dur = steps
            .get(i)
            .map(|(a, b)| b.duration_since(*a).as_secs_f64())
            .unwrap_or(0.0);
        parts.push(format!(
            "{}\n    {m}\n",
            make_header(
                &format!("{}\n  ({})", shlex::join(step), seconds_str(dur)),
                4
            )
        ));
    }
    msg.push_str(&parts.join("\n"));
    let mut msg = msg.trim_end_matches('\n').to_string();
    if let Some(last) = results.last()
        && !last.success
    {
        let fail_step = &chained[results.len() - 1];
        msg.push_str(&format!(
            "\n\nFailed to complete step:\n\n{}\n    {}",
            make_header(&shlex::join(fail_step), 4),
            last.message
        ));
    }
    if let Some(t) = total {
        let n = successes.len();
        let header = format!("| {n} step{} in {} |", plural(n), seconds_str(t));
        msg.push_str(&format!(
            "\n\n\n    {}\n    {header}\n    {}",
            "-".repeat(header.len()),
            "-".repeat(header.len())
        ));
    }
    SuccessTuple {
        success,
        message: msg.trim_end().to_string(),
    }
}

fn action_words(args: &ArgsMap) -> Vec<String> {
    args.get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default()
}

/// `entry_with_args(**args)`: resolve the action function and run it.
pub fn entry_with_args(mut args: ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
    let sysargs: Vec<String> = args
        .get("sysargs")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    if contains(&sysargs, keys::AND) {
        let sysargs: Vec<String> = if contains(&sysargs, "-d") || contains(&sysargs, "--daemon") {
            sysargs
                .iter()
                .map(|a| {
                    if a == keys::AND {
                        keys::ESCAPED_AND.into()
                    } else {
                        a.clone()
                    }
                })
                .collect()
        } else {
            sysargs
        };
        return entry_at_depth(&sysargs, rt, depth + 1);
    }

    let mut action = action_words(&args);
    if action.is_empty() || (action[0] == "mrsm" && action.len() == 1) {
        if rt.in_shell {
            return SuccessTuple::success();
        }
        let instance = match args.get("mrsm_instance") {
            Some(PyValue::Str(s)) => Some(s.clone()),
            _ => None,
        };
        crate::shell::run(&sysargs, instance.as_deref(), rt);
        return SuccessTuple::success();
    }

    if let Ok(manifest) = crate::python::manifest(rt)
        && let Err(e) = crate::python::arguments(&mut args, &manifest, rt)
    {
        return SuccessTuple::fail(format!("Invalid arguments:\n    {}", e.message));
    }
    let mut skip_schedule = false;
    let executor_keys = args
        .get("executor_keys")
        .map(PyValue::py_str)
        .unwrap_or_else(|| "local".into());
    if executor_keys.starts_with("api:") {
        let accepts =
            actions::get_action(&action, CORE_ACTIONS).is_some_and(|r| r.accepts_executor_keys());
        if !accepts {
            let label = executor_keys
                .rsplit(':')
                .next()
                .unwrap_or_default()
                .to_string();
            action.insert(0, "api".into());
            action.insert(1, label);
            skip_schedule = true;
        }
    }
    if args.get("daemon").is_some_and(PyValue::is_truthy) && action[0] != "stack" {
        action.splice(0..0, ["start".to_string(), "jobs".to_string()]);
    }
    if let Ok(manifest) = crate::python::manifest(rt)
        && !args.get("daemon").is_some_and(PyValue::is_truthy)
        && let Ok(natives) = native::session(rt).native_plugins(rt.config())
        && let Some((plugin, name)) =
            crate::native_plugins::action_owner(&action, &manifest, &natives)
    {
        // Native (C-ABI) plugin actions run in-process; they shadow Python plugin actions.
        env::apply_env_arguments(&mut args, rt.env);
        args.insert("action".into(), PyValue::str_list(&action));
        return crate::native_plugins::run_action(&plugin, &name, &args, &natives)
            .unwrap_or_else(|e| SuccessTuple::fail(format!("{}: {}", e.class, e.message)));
    }
    if let Ok(manifest) = crate::python::manifest(rt)
        && crate::python::action_name(&action, &manifest).is_some()
        && !args.get("daemon").is_some_and(PyValue::is_truthy)
    {
        env::apply_env_arguments(&mut args, rt.env);
        return crate::python::plugin_action(&args, rt)
            .unwrap_or_else(|e| SuccessTuple::fail(format!("{}: {}", e.class, e.message)));
    }
    let resolved: Resolved = match actions::get_action(&action, CORE_ACTIONS) {
        Some(r) => r,
        None => {
            // Unknown actions run as shell commands in v4.
            action.insert(0, "sh".into());
            match actions::get_action(&["sh"], CORE_ACTIONS) {
                Some(r) => r,
                None => return SuccessTuple::fail("action `sh` is not available"),
            }
        }
    };
    if action[0] == "start" && action.get(1).is_some_and(|a| a == "job" || a == "jobs") {
        skip_schedule = true;
    }
    let remaining = actions::remove_leading_action(&action, CORE_ACTIONS);
    args.insert("action".into(), PyValue::str_list(&remaining));
    env::apply_env_arguments(&mut args, rt.env);

    if let Some(schedule) = args.get("schedule").filter(|v| v.is_truthy())
        && !skip_schedule
    {
        let schedule = schedule.py_str();
        return crate::native::jobs::run_schedule(rt, &schedule, |rt| {
            run_action(&resolved, &args, rt)
        });
    }
    run_action(&resolved, &args, rt)
}

fn run_action(resolved: &Resolved, args: &ArgsMap, rt: &mut Runtime<'_>) -> SuccessTuple {
    let result: ActionResult = match resolved.function_name() {
        "_show_version" => builtin::show_version(args, rt),
        "_show_arguments" => builtin::show_arguments(args, rt),
        "_show_help" => builtin::show_help(args, rt),
        "_show_actions" => builtin::show_actions(args, rt),
        "_show_config" => builtin::show_config(args, rt),
        "_show_environment" => builtin::show_environment(args, rt),
        "_show_plugins" => builtin::show_plugins(args, rt),
        "_edit_config" => builtin::edit_config(args, rt),
        "reload" => builtin::reload(args, rt),
        "compose" => crate::compose_action::compose_action(args, rt),
        "python" => builtin::python(args, rt),
        "_start_api" => crate::server::start_api(args, rt),
        "_start_mcp" => crate::server::start_mcp(args, rt),
        "sh" => builtin::sh(args, rt),
        "clear" => builtin::clear(args, rt),
        other => match crate::native::run(other, args, rt) {
            Some(r) => r,
            None => crate::python::core_action(&resolved.display_name(), args, rt),
        },
    };
    match result {
        Ok(tup) => tup,
        Err(e) => action_failure(resolved, args, &e, rt),
    }
}

/// `_do_action_wrapper`'s message when the action raises.
fn action_failure(
    resolved: &Resolved,
    args: &ArgsMap,
    e: &ActionError,
    rt: &mut Runtime<'_>,
) -> SuccessTuple {
    let mut name_parts: Vec<String> = resolved
        .function_name()
        .split('_')
        .map(str::to_string)
        .collect();
    name_parts.extend(action_words(args));
    let action_name = name_parts.join(" ");
    // PARITY: `kw.get('debug', True)`: the traceback is logged unless `--debug` was parsed
    // as False; the Rust CLI prints the exception line instead of a Python traceback.
    let log_traceback = args.get("debug").is_none_or(PyValue::is_truthy);
    if log_traceback {
        rt.eprint(&format!("{}: {}\n", e.class, e.message));
    }
    let mut msg = format!(
        "Failed to execute `{}` with `{}`",
        action_name.trim(),
        e.class
    );
    if e.message.is_empty() {
        msg.push('.');
    } else {
        msg.push_str(&format!(":\n\n{}", e.message));
    }
    if !log_traceback {
        msg.push_str("\n\nRun again with '--debug' to see a full stacktrace.");
    }
    SuccessTuple::fail(msg)
}

/// Render the arguments map as a Python `dict` literal, one key per line.
pub fn format_args_map(args: &ArgsMap) -> String {
    if args.is_empty() {
        return "{}".into();
    }
    let lines: Vec<String> = args
        .iter()
        .map(|(k, v)| format!("    {}: {},", crate::value::repr_str(k), v.py_repr()))
        .collect();
    format!("{{\n{}\n}}", lines.join("\n"))
}

/// `parse_help(sysargs)`: choose which help message to print.
pub fn parse_help(sysargs: &[String], rt: &mut Runtime<'_>) {
    let args = parse_arguments(sysargs, &rt.dates).unwrap_or_default();
    let action = action_words(&args);
    if let Ok(manifest) = crate::python::manifest(rt)
        && let Some(name) = crate::python::action_name(&action, &manifest)
        && let Some(info) = manifest
            .plugins
            .values()
            .flat_map(|p| &p.actions)
            .find(|a| a.name == name)
    {
        rt.print(&format!("{}\n", info.doc.as_deref().unwrap_or_default()));
        return;
    }
    if action.is_empty() {
        let _ = builtin::show_help(&args, rt);
        return;
    }
    match help::format_action_help(&action) {
        Some(text) => rt.print(&text),
        None => {
            // `actions['show'](['actions'], **_args)`.
            let mut kw = args.clone();
            kw.shift_remove("action");
            let opts = OptionsArgs {
                name: "actions".into(),
                sort_options: true,
                nopretty: kw.get("nopretty").is_some_and(PyValue::is_truthy),
                ..OptionsArgs::default()
            };
            let names = builtin::listed_actions();
            let text = rt.with_formatter(|f| f.print_options(&names, &opts));
            rt.print(&text);
        }
    }
}

/// `parse_version(sysargs)`.
pub fn parse_version(sysargs: &[String], rt: &mut Runtime<'_>) {
    let version = crate::VERSION;
    let _ = if contains(sysargs, "--nopretty") {
        writeln!(rt.out, "{version}")
    } else {
        writeln!(rt.out, "Meerschaum v{version}")
    };
}
