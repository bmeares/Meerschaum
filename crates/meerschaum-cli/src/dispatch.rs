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

    /// Run a process with its output captured (`subprocess.Popen(..., stdout=PIPE,
    /// stderr=PIPE).communicate(timeout=...)`). The default runs it attached and captures
    /// nothing.
    fn output(
        &mut self,
        spec: &CommandSpec,
        _timeout: Option<std::time::Duration>,
    ) -> std::io::Result<CommandOutput> {
        let code = self.run(spec)?;
        Ok(CommandOutput {
            code,
            ..CommandOutput::default()
        })
    }

    /// A multi-line answer (`prompt(multiline=True)`: `[Esc + Enter]` submits). `None` at the end
    /// of input.
    fn read_multiline(&mut self, prompt: &str) -> Option<String> {
        self.read_line(prompt)
    }

    /// `timed_input(seconds)`: a line typed within `timeout`, else `None`.
    fn read_line_timeout(&mut self, _timeout: std::time::Duration) -> Option<String> {
        None
    }

    /// Like [`Host::read_line`], but a terminal host reports Ctrl-C through
    /// `native::terminal::take_interrupt` (`KeyboardInterrupt` in v4) instead of ending the
    /// process.
    fn read_line_interruptible(&mut self, prompt: &str) -> Option<String> {
        self.read_line(prompt)
    }

    /// Like [`Host::read_line`] without echoing the input (passwords).
    fn read_secret(&mut self, prompt: &str) -> Option<String> {
        self.read_line(prompt)
    }

    /// Like [`Host::read_line_prefilled`] with a dropdown of `items` which filters as the user
    /// types (`bootstrap pipes`: connector, metric and location keys). Any text is accepted, not
    /// only the items. Without a terminal it is a plain line.
    fn read_line_suggesting(
        &mut self,
        prompt: &str,
        _items: &[String],
        prefill: &str,
    ) -> Option<String> {
        self.read_line_prefilled(prompt, prefill)
    }

    /// Like [`Host::read_line`] with `prefill` already typed into the editable buffer
    /// (`prompt_toolkit.prompt(default=...)`). Without a terminal the typed line is appended to
    /// the prefill like `prompt_toolkit` does for piped input.
    fn read_line_prefilled(&mut self, prompt: &str, prefill: &str) -> Option<String> {
        self.read_line(prompt)
            .map(|line| format!("{prefill}{line}"))
    }
}

/// What a captured process produced.
#[derive(Debug, Clone, Default)]
pub struct CommandOutput {
    /// Standard output.
    pub stdout: Vec<u8>,
    /// Standard error.
    pub stderr: Vec<u8>,
    /// The exit code (`-N` after signal N, like `Popen.returncode`).
    pub code: i32,
    /// Set when the timeout elapsed (the process was killed): `str(TimeoutExpired)`.
    pub timed_out: bool,
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
        // The program shares the terminal's foreground process group, so Ctrl-C reaches it
        // directly; this process waits (see `shell::interrupt`). A second Ctrl-C within two
        // seconds kills it, unless it is an interactive session.
        let mut child = cmd.spawn()?;
        let interactive = spec.args.is_empty()
            || spec
                .args
                .iter()
                .any(|a| a == "-i" || a == "ptpython" || a == "--interactive");
        let scope = crate::shell::interrupt::ChildScope::enter(Some(child.id()), !interactive);
        let status = child.wait();
        drop(scope);
        Ok(status?.code().unwrap_or(1))
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            let answer = crate::native::terminal::read_prefilled(prompt, "");
            if crate::native::terminal::take_interrupt() {
                std::panic::resume_unwind(Box::new(crate::shell::interrupt::Interrupted));
            }
            return answer;
        }
        print!("{prompt}");
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line.trim_end_matches(['\n', '\r']).to_string()),
        }
    }

    fn output(
        &mut self,
        spec: &CommandSpec,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<CommandOutput> {
        crate::native::terminal::capture(spec, timeout)
    }

    fn read_line_timeout(&mut self, timeout: std::time::Duration) -> Option<String> {
        crate::native::terminal::timed_input(timeout)
    }

    fn read_multiline(&mut self, prompt: &str) -> Option<String> {
        // PARITY: `prompt_toolkit` cannot submit a multiline prompt from piped input and ends
        // with `EOFError`.
        if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            return None;
        }
        crate::native::terminal::read_multiline(prompt)
    }

    fn read_line_interruptible(&mut self, prompt: &str) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            return crate::native::terminal::read_prefilled(prompt, "");
        }
        self.read_line(prompt)
    }

    fn read_secret(&mut self, prompt: &str) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            crate::native::terminal::read_secret(prompt)
        } else {
            self.read_line(prompt)
        }
    }

    fn read_line_suggesting(
        &mut self,
        prompt: &str,
        items: &[String],
        prefill: &str,
    ) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin())
            && std::io::IsTerminal::is_terminal(&std::io::stdout())
        {
            let ansi = std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty());
            return crate::native::terminal::read_suggested(prompt, items, prefill, ansi);
        }
        self.read_line_prefilled(prompt, prefill)
    }

    fn read_line_prefilled(&mut self, prompt: &str, prefill: &str) -> Option<String> {
        if prefill.is_empty() || !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            return self
                .read_line(prompt)
                .map(|line| format!("{prefill}{line}"));
        }
        crate::native::terminal::read_prefilled(prompt, prefill)
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

    fn read_line_timeout(&mut self, _timeout: std::time::Duration) -> Option<String> {
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
    /// Where waiting feedback (spinners, [`crate::format::progress`]) is drawn. Only the process
    /// runtime turns it on (stderr is a terminal); tests and piped output never see it.
    pub progress: crate::format::progress::ProgressSink,
    /// Startup owns read-only manifest mode until the first doctor step (or an early return).
    /// The value records whether that startup created the config registry.
    doctor_startup: Option<bool>,
}

fn no_vars() -> Vec<(String, String)> {
    Vec::new()
}

/// The process environment as strings, in order (variables that are not valid UTF-8 are
/// skipped: `std::env::vars()` panics on them).
pub fn process_vars() -> Vec<(String, String)> {
    meerschaum_core::process_env::vars()
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
            progress: crate::format::progress::ProgressSink::stderr_if_tty(),
            doctor_startup: None,
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
            progress: crate::format::progress::ProgressSink::Off,
            doctor_startup: None,
        }
    }

    /// The config registry.
    pub fn config(&mut self) -> &Config {
        self.config.get_or_insert_with(|| Self::build_config(true))
    }

    /// The config registry, created (when it does not exist yet) so that it never writes a file
    /// (`write_missing` defaults, `formatting.json`): what `doctor` promises. A registry that
    /// already exists is returned as it is.
    pub fn config_read_only(&mut self) -> &Config {
        self.config.get_or_insert_with(|| Self::build_config(false))
    }

    fn build_config(allow_writes: bool) -> Config {
        {
            let mut options = ConfigOptions::from_process();
            options.allow_writes = allow_writes;
            Config::new(options).unwrap_or_else(|e| {
                use meerschaum_core::config::ConfigError;
                match &e {
                    ConfigError::DefaultRootRefused { .. } | ConfigError::EmptyPath { .. } => {
                        eprintln!("{e}");
                        std::process::exit(1);
                    }
                    // PARITY: `config/_paths.py` prints this and exits 1 for a missing
                    // `MRSM_ROOT_DIR` (and the other `MRSM_*` path variables).
                    ConfigError::InvalidPath { var, path } => {
                        match var.as_str() {
                            "MRSM_ROOT_DIR" => eprintln!(
                                "Invalid root directory '{path}' set for environment variable \
                                 '{var}'.\nPlease enter a valid path for {var}."
                            ),
                            "MRSM_CONFIG_DIR" => eprintln!(
                                "Invalid configuration directory '{path}' set for environment \
                                 variable '{var}'\nPlease enter a valid path for {var}."
                            ),
                            _ => eprintln!("{e}\nPlease enter a valid path for {var}."),
                        }
                        std::process::exit(1);
                    }
                    _ => {}
                }
                let mut opts = ConfigOptions::from_process();
                opts.allow_writes = false;
                Config::new(opts).expect("config registry")
            })
        }
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
        // A long loop that prints as it goes stops promptly after Ctrl-C.
        crate::shell::interrupt::check();
        crate::format::progress::suspend();
        crate::native::api_warnings::flush(self);
        let _ = self.out.write_all(text.as_bytes());
        let _ = self.out.flush();
        crate::format::progress::resume(text.is_empty() || text.ends_with('\n'));
    }

    /// Write to stderr.
    pub fn eprint(&mut self, text: &str) {
        crate::shell::interrupt::check();
        crate::format::progress::suspend();
        crate::native::api_warnings::flush(self);
        let _ = self.err.write_all(text.as_bytes());
        let _ = self.err.flush();
        crate::format::progress::resume(text.is_empty() || text.ends_with('\n'));
    }
}

fn contains(sysargs: &[String], flag: &str) -> bool {
    sysargs.iter().any(|a| a == flag)
}

/// `meerschaum.__main__.main`: returns the process exit code.
pub fn main(sysargs: &[String], rt: &mut Runtime<'_>) -> i32 {
    // `doctor` is read-only: from the first config read on, nothing is written and the plugin
    // manifest is the cached one (never validated or rebuilt, which imports plugin code).
    if starts_with_doctor(sysargs, &rt.dates, None).unwrap_or(true) {
        let config_created = rt.config.is_none();
        rt.config_read_only();
        let session = native::session(rt);
        if !session.cached_manifest_only() {
            session.set_cached_manifest_only(true);
            rt.doctor_startup = Some(config_created);
        }
    }
    let code = main_inner(sysargs, rt);
    // Parse errors, help, version and schema output can return before a doctor step consumes
    // startup ownership. Always release it, including when this runtime is reused by a host.
    if let Some(config_created) = rt.doctor_startup.take() {
        end_doctor_mode(rt, config_created);
    }
    code
}

fn main_inner(sysargs: &[String], rt: &mut Runtime<'_>) -> i32 {
    // `--version` and `--help` never start a Python worker: they are answered before the plugin
    // trees are validated (which probes the interpreter and may import `meerschaum`), and help
    // reads the plugin actions the last command cached instead of validating the trees.
    if contains(sysargs, PRINT_ARG_SCHEMA_FLAG) {
        let schema = serde_json::to_string_pretty(&schema::arg_schema()).unwrap_or_default();
        let _ = writeln!(rt.out, "{schema}");
        return 0;
    }
    if contains(sysargs, "--help") || contains(sysargs, "-h") {
        let session = native::session(rt);
        let was_cached_only = session.cached_manifest_only();
        session.set_cached_manifest_only(true);
        parse_help(sysargs, rt);
        session.set_cached_manifest_only(was_cached_only);
        return 0;
    }
    if contains(sysargs, "--version") || contains(sysargs, "-V") {
        parse_version(sysargs, rt);
        return 0;
    }
    let progress = crate::format::progress::Progress::start_flags(
        rt,
        contains(sysargs, "--nopretty"),
        contains(sysargs, "--debug"),
        "Loading plugins",
    );
    let manifest = crate::python::manifest(rt);
    drop(progress);
    let manifest_error = manifest.err().map(|e| e.message);
    let nopretty = contains(sysargs, "--nopretty");
    let in_job = (rt.env)(meerschaum_jobs::supervisor::RESULT_PATH_ENV).is_some();
    if (sysargs.is_empty() || contains(sysargs, "--shell")) && !in_job {
        if let Some(message) = &manifest_error {
            warn_manifest_error(rt, message);
        }
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
    if let Some(message) = &manifest_error {
        warn_manifest_error_unless_reported(rt, message, &result);
    }
    if crate::shell::interrupt::take_interrupted(&result) && !in_job {
        // Ctrl-C ended the command: exit 130 like a program killed by SIGINT, with no traceback.
        rt.eprint(&format!("\n{}\n", crate::shell::interrupt::MESSAGE));
        return crate::shell::interrupt::EXIT_CODE;
    }
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
        // PARITY: `Daemon._run_exit` ends the job's log with `mrsm.pprint(result)` (a tuple is
        // printed by `print_tuple` with its default arguments, no padding).
        let text = rt.with_formatter(|f| f.print_tuple(&result, TupleArgs::default()));
        rt.print(&text);
        return if result.success { 0 } else { 1 };
    }
    // PARITY: v4 only sets a failing exit code when the tuple is printed, so `--nopretty`
    // always exited 0 (`meerschaum/__main__.py` `main`). v5 exits 1 on failure (wave 18, D-10)
    // so scripts can rely on the status; nothing is printed, as in v4.
    if nopretty {
        return if result.success { 0 } else { 1 };
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

/// Whether the first effective dispatched step is `doctor` or `compose explain`, after chaining, pipeline rewriting
/// and argument patches. SDK hosts use this before loading plugins; the step itself owns the
/// read-only mode, so later chained actions retain normal plugin discovery.
/// A parsing error means no action can execute and also needs no eager plugin loading.
pub fn starts_with_doctor(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
) -> Result<bool, PyException> {
    let mut doctor = false;
    // Reuse the real parser, stopping before any action executes or later steps are parsed.
    run_chain_inner(sysargs, dates, patch_args, false, |args| {
        doctor = is_read_only_action(&action_words(&args));
        SuccessTuple::fail("")
    })?;
    Ok(doctor)
}

/// `entry_without_daemon` without the shell's executor keys and the systemd result file:
/// split `:` pipeline flags and `+` chains, parse each step and run `execute` until one fails.
pub fn run_chain<F>(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
    execute: F,
) -> Result<SuccessTuple, PyException>
where
    F: FnMut(ArgsMap) -> SuccessTuple,
{
    run_chain_inner(sysargs, dates, patch_args, true, execute)
}

fn run_chain_inner<F>(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
    validate_arguments: bool,
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
        if validate_arguments && let Some(PyValue::Exception(e)) = args.get(keys::FAILURE_KEY) {
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

/// The actions that promise to write nothing on a fresh root: `doctor`, and `compose explain`
/// (side effect free by design: no `config/`, no `cache/`, no root initialization).
fn is_read_only_action(words: &[String]) -> bool {
    match words {
        [first, ..] if first == "doctor" => true,
        [first, second, ..] => first == "compose" && second == "explain",
        _ => false,
    }
}

fn end_doctor_mode(rt: &mut Runtime<'_>, config_created: bool) {
    native::session(rt).set_cached_manifest_only(false);
    if config_created {
        rt.config().with_state(|s| s.set_allow_writes(true));
    }
}

/// `entry_with_args(**args)`: resolve the action function and run it.
///
/// `doctor` runs with the cached plugin manifest and a config that never writes (see [`main`]).
/// Both are undone when its step ends, whoever started them: the manifest state is dropped (the
/// next step validates the plugins again) and a registry the read-only start created is made
/// writable again, as [`Runtime::config`] would have built it.
pub fn entry_with_args(args: ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
    // Ctrl-C cancels this action (the whole `mrsm` program, or the shell's command) and nothing
    // else: see `shell::interrupt`. A job worker keeps its stop-flag semantics.
    let session = rt.native.clone();
    let job = (rt.env)(meerschaum_jobs::supervisor::RESULT_PATH_ENV).is_some();
    crate::shell::interrupt::guarded(session.as_ref(), job, || {
        entry_with_args_doctor(args, rt, depth)
    })
}

fn entry_with_args_doctor(args: ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
    if !is_read_only_action(&action_words(&args)) {
        return entry_with_args_inner(args, rt, depth);
    }
    let session = native::session(rt);
    let startup = rt.doctor_startup.take();
    if session.cached_manifest_only() && startup.is_none() {
        // An outer caller set the mode up and owns it.
        return entry_with_args_inner(args, rt, depth);
    }
    let config_created = startup.unwrap_or_else(|| rt.config.is_none());
    rt.config_read_only();
    session.set_cached_manifest_only(true);
    let result = entry_with_args_inner(args, rt, depth);
    end_doctor_mode(rt, config_created);
    result
}

fn entry_with_args_inner(mut args: ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
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
        // A job has no terminal: a worker whose sysargs hold no action (`mrsm -y`) fails
        // instead of opening a shell that would wait on stdin forever.
        if (rt.env)(meerschaum_jobs::supervisor::RESULT_PATH_ENV).is_some() {
            return SuccessTuple::fail(
                "The job has no action to run (an interactive shell cannot start in a job).",
            );
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
        // The terminal gives the worker the Ctrl-C (its call raises `KeyboardInterrupt` and
        // returns); a second one within two seconds drops the call, which kills the worker.
        let _waiting = crate::shell::interrupt::ChildScope::enter(None, true);
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
        // `--jitter 30s` (v5): the schedule's fixed offset; `jobs.schedule.jitter_seconds` else.
        let jitter = args
            .get("jitter")
            .map(PyValue::py_str)
            .and_then(|v| meerschaum_jobs::schedule::parse_jitter_seconds(&v));
        return crate::native::jobs::run_schedule_jittered(rt, &schedule, jitter, |rt| {
            run_action(&resolved, &args, rt, depth)
        });
    }
    run_action(&resolved, &args, rt, depth)
}

fn run_action(
    resolved: &Resolved,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    depth: usize,
) -> SuccessTuple {
    native::drivers::register_install_reporter(args, rt);
    let result: ActionResult = match resolved.function_name() {
        "_start_pipeline" => run_pipeline(args, rt, depth),
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
        // A server never finishes a "first lap": a job's startup slot (D23) is freed as it starts.
        "_start_api" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_api(args, rt)
        }
        "_start_mcp" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_mcp(args, rt)
        }
        "_start_webterm" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_webterm(args, rt)
        }
        "sh" => builtin::sh(args, rt),
        "clear" => builtin::clear(args, rt),
        other if native::residual::handles(other) => native::residual::run(other, args, rt)
            .unwrap_or_else(|| Ok(SuccessTuple::fail("Unhandled action."))),
        other => match crate::native::run(other, args, rt) {
            Some(r) => r,
            None => Err(ActionError::new(
                "NotImplementedError",
                format!(
                    "The action `{}` has no native implementation.",
                    resolved.display_name()
                ),
            )),
        },
    };
    match result {
        Ok(tup) => tup,
        Err(e) => action_failure(resolved, args, &e, rt),
    }
}

/// Keep `:` chains in the native dispatcher: core steps must not import the Python core
/// (and its formatting / scheduling dependencies) simply because they form a pipeline.
fn run_pipeline(args: &ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> ActionResult {
    let params = match args.get("params") {
        Some(PyValue::Dict(values)) => values,
        _ => return Ok(SuccessTuple::fail("Nothing to do.")),
    };
    let get_param = |key: &str| {
        params
            .iter()
            .find(|(k, _)| k.as_str() == Some(key))
            .map(|(_, v)| v)
    };
    let Some(PyValue::Str(line)) = get_param("sub_args_line").filter(|v| v.is_truthy()) else {
        return Ok(SuccessTuple::fail("Nothing to do."));
    };
    if depth >= MAX_ENTRY_DEPTH {
        return Ok(SuccessTuple::fail(
            "RecursionError: maximum recursion depth exceeded",
        ));
    }
    let sysargs = shlex::split(line).map_err(|e| ActionError::new("ValueError", e))?;
    let patch: ArgsMap = match get_param("patch_args") {
        Some(PyValue::Dict(values)) => values
            .iter()
            .map(|(k, v)| (k.py_str(), v.clone()))
            .collect(),
        _ => ArgsMap::new(),
    };
    let timeout = native::float(args, "timeout_seconds");
    let timeout_text = args
        .get("timeout_seconds")
        .map(PyValue::py_str)
        .unwrap_or_default();
    let patch_json = match get_param("patch_args") {
        Some(value @ PyValue::Dict(_)) => value
            .json_dumps_compact()
            .map_err(|e| ActionError::new(&e.class, e.message))?,
        _ => "null".to_string(),
    };
    let looping = native::flag(args, "loop");
    let count = native::words(args, "action")
        .first()
        .and_then(|s| s.trim_start_matches('x').parse::<i64>().ok())
        .unwrap_or(if looping { -1 } else { 1 });
    let delay = native::float(args, "min_seconds").unwrap_or(1.0);
    let stop = native::jobs::stop_requested_flag();
    native::jobs::install_stop_handler(&native::session(rt));
    let mut ran = 0;
    let mut result = SuccessTuple::fail("Did not pipeline.");
    loop {
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            native::warn(rt, "Cancelled pipeline.");
            if result.message == "Did not pipeline." {
                result.message = "Pipeline was cancelled.".into();
            }
            break;
        }
        let dates = rt.dates.clone();
        result = match timeout {
            None => match run_chain(&sysargs, &dates, Some(&patch), |args| {
                entry_with_args(args, rt, depth + 1)
            }) {
                Ok(result) => result,
                Err(error) => SuccessTuple::fail(format!("{}: {}", error.class, error.message)),
            },
            // PARITY: with `--timeout-seconds` v4 runs each step in a child interpreter and
            // kills it when the time is up; here the child is this binary.
            Some(seconds) => {
                match timed_pipeline_step(rt, line, &patch_json, seconds, &timeout_text) {
                    Ok(Some(done)) => done,
                    Ok(None) => result.clone(),
                    Err(error) => {
                        native::warn(rt, &error.message);
                        result.clone()
                    }
                }
            }
        };
        ran += 1;
        // A job's startup slot (D23) is held until its first lap is done.
        meerschaum_jobs::startup::signal_first_lap_done();
        // PARITY: v4 sleeps between attempts, including after failures, and `--loop`
        // overrides the finite `xN` count (`actions/start.py::_start_pipeline`).
        if !looping && count == 1 {
            break;
        }
        if delay != 0.0 && ran != count {
            native::info(
                rt,
                &format!(
                    "Sleeping for {} seconds...",
                    args.get("min_seconds")
                        .map(PyValue::py_str)
                        .unwrap_or_else(|| "1.0".into())
                ),
            );
            native::jobs::sleep_unless_stopped(delay);
        }
        if !looping && ran >= count {
            break;
        }
    }
    if count != 1 {
        native::info(
            rt,
            &format!(
                "Ran pipeline {ran} time{}.",
                if ran == 1 { "" } else { "s" }
            ),
        );
    }
    Ok(result)
}

/// The first argument of the child `start pipeline --timeout-seconds` runs each step in.
pub const PIPELINE_CHILD_ARG: &str = "__mrsm_pipeline_child__";
const FENCE_BEGIN: &str = "<MRSM_RESULT>";
const FENCE_END: &str = "</MRSM_RESULT>";

/// One `start pipeline` step in a child process, killed after `seconds` (`poll_process` in
/// v4): its output is printed as it arrives and its result is read from the fenced JSON line.
/// `Ok(None)`: the child ended without a result (v4 keeps the previous one).
fn timed_pipeline_step(
    rt: &mut Runtime<'_>,
    line: &str,
    patch_json: &str,
    seconds: f64,
    seconds_text: &str,
) -> Result<Option<SuccessTuple>, ActionError> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    // This program again: the `mrsm` binary, or `python -m meerschaum` in the extension.
    let (program, leading) = meerschaum_core::self_exe::self_program();
    let mut command = Command::new(program);
    command
        .args(leading)
        .arg(PIPELINE_CHILD_ARG)
        .arg(line)
        .arg(patch_json)
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // v4 starts the step with `os.setsid` and stops it with `os.killpg`: the step leads its own
    // session (and process group), so its children die with it and, having no controlling
    // terminal, a step that prompts is not stopped by SIGTTIN.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: the closure runs between `fork` and `exec` and only calls `setsid(2)`, which
        // is async-signal-safe; it allocates nothing and takes no locks.
        #[allow(unsafe_code)]
        unsafe {
            command.pre_exec(|| {
                rustix::process::setsid()
                    .map(|_| ())
                    .map_err(std::io::Error::from)
            });
        }
    }
    for (key, value) in (rt.vars)() {
        command.env(key, value);
    }
    let mut child = command
        .spawn()
        .map_err(|e| ActionError::new("OSError", format!("Failed to start the pipeline: {e}")))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let (sender, receiver) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(seconds.max(0.0));
    let mut result: Option<SuccessTuple> = None;
    let handle = |text: String, rt: &mut Runtime<'_>, result: &mut Option<SuccessTuple>| {
        if let Some(begin) = text.find(FENCE_BEGIN) {
            let from = begin + FENCE_BEGIN.len();
            let end = text.find(FENCE_END).unwrap_or(text.len());
            if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(&text[from..end])
                && let (Some(success), Some(message)) = (
                    items.first().and_then(serde_json::Value::as_bool),
                    items.get(1).and_then(serde_json::Value::as_str),
                )
            {
                *result = Some(SuccessTuple {
                    success,
                    message: message.to_string(),
                });
            }
            return;
        }
        rt.print(&format!("{text}\n"));
    };
    loop {
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        match receiver.recv_timeout(wait.min(std::time::Duration::from_millis(100))) {
            Ok(text) => handle(text, rt, &mut result),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if std::time::Instant::now() >= deadline {
                    // The step leads its own process group (`setsid`, unix only): SIGKILL the
                    // whole group. Elsewhere only the direct child is killed (below), and
                    // processes it started may outlive the timeout.
                    #[cfg(unix)]
                    if let Some(pgid) = rustix::process::Pid::from_raw(child.id() as i32) {
                        let _ = rustix::process::kill_process_group(
                            pgid,
                            rustix::process::Signal::KILL,
                        );
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                    result = Some(SuccessTuple::fail(format!(
                        "Failed to execute pipeline within {seconds_text} second{}.",
                        if seconds != 1.0 { "s" } else { "" }
                    )));
                    return Ok(result);
                }
            }
        }
    }
    let _ = child.wait();
    Ok(result)
}

/// The first argument of a child that runs an in-process command of Compose (v4's
/// `run_mrsm_command(..., _subprocess=False)`, which calls `entry(sysargs)` and returns the
/// result without printing it): `mrsm __mrsm_entry_child__ <result path> <sysargs...>`. It is
/// honored only when [`ENTRY_CHILD_ENV`] names the same path (the compose runner sets both).
pub const ENTRY_CHILD_ARG: &str = "__mrsm_entry_child__";

/// The environment marker of an [`ENTRY_CHILD_ARG`] child: the result path. The process entry
/// takes it out of the environment before running anything (`run::take_entry_child_marker`), so
/// the command's own children (jobs, workers) never see it.
pub const ENTRY_CHILD_ENV: &str = "MRSM__ENTRY_CHILD";

/// The child of [`ENTRY_CHILD_ARG`]: run `entry(sysargs)`, write the result as
/// `[success, message]` JSON to the result path (a new file, 0600 on Unix, in the private
/// directory the parent created) and print nothing of it. The exit code is 0 on success, else 1.
pub fn entry_child(args: &[String], rt: &mut Runtime<'_>) -> i32 {
    let Some((path, sysargs)) = args.split_first() else {
        return 1;
    };
    let manifest_error = crate::python::manifest(rt).err().map(|e| e.message);
    let result = entry(sysargs, rt);
    if let Some(message) = &manifest_error {
        warn_manifest_error_unless_reported(rt, message, &result);
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    if let Ok(mut file) = options.open(path) {
        let _ = file.write_all(fence_json(&result).as_bytes());
    }
    if result.success { 0 } else { 1 }
}

/// Read the result an [`entry_child`] wrote to `path` (`None` when it wrote none).
pub fn read_entry_child_result(path: &std::path::Path) -> Option<SuccessTuple> {
    let text = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let items = value.as_array()?;
    Some(SuccessTuple {
        success: items.first()?.as_bool()?,
        message: items.get(1)?.as_str()?.to_string(),
    })
}

/// The child of [`timed_pipeline_step`]: run `line` with the patch arguments and print the result
/// between the fences.
pub fn pipeline_child(args: &[String], rt: &mut Runtime<'_>) -> i32 {
    let line = args.first().cloned().unwrap_or_default();
    let patch_json = args.get(1).cloned().unwrap_or_default();
    let sysargs = match shlex::split(&line) {
        Ok(sysargs) => sysargs,
        Err(e) => {
            println!(
                "{FENCE_BEGIN}{}{FENCE_END}",
                fence_json(&SuccessTuple::fail(e))
            );
            return 0;
        }
    };
    let patch: ArgsMap = match serde_json::from_str::<serde_json::Value>(&patch_json) {
        Ok(serde_json::Value::Object(map)) => {
            map.into_iter().map(|(k, v)| (k, json_to_py(&v))).collect()
        }
        _ => ArgsMap::new(),
    };
    let result = entry_with_patch(&sysargs, &patch, rt);
    println!("{FENCE_BEGIN}{}{FENCE_END}", fence_json(&result));
    0
}

fn fence_json(result: &SuccessTuple) -> String {
    serde_json::json!([result.success, result.message]).to_string()
}

fn json_to_py(value: &serde_json::Value) -> PyValue {
    match value {
        serde_json::Value::Null => PyValue::None,
        serde_json::Value::Bool(b) => PyValue::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => PyValue::Int(i128::from(i)),
            None => PyValue::Float(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(s) => PyValue::Str(s.clone()),
        serde_json::Value::Array(items) => PyValue::List(items.iter().map(json_to_py).collect()),
        serde_json::Value::Object(map) => PyValue::Dict(
            map.iter()
                .map(|(k, v)| (PyValue::Str(k.clone()), json_to_py(v)))
                .collect(),
        ),
    }
}

/// Print `Unable to load plugin manifest: <message>`.
fn warn_manifest_error(rt: &mut Runtime<'_>, message: &str) {
    rt.eprint(&format!("Unable to load plugin manifest: {message}\n"));
}

/// The manifest warning of a one-shot command, printed after the action ran: not at all when the
/// action's own result already says it (a refused Python worker fails the manifest load and the
/// action with the same text; one message is enough).
fn warn_manifest_error_unless_reported(rt: &mut Runtime<'_>, message: &str, result: &SuccessTuple) {
    if !result.message.contains(message) {
        warn_manifest_error(rt, message);
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
    // A worker error (a refused or unstartable Python interpreter) has no v4 traceback to
    // mirror: its text is printed once, by the result below (the start-up manifest warning of
    // the same failure is dropped, see `warn_manifest_error_unless_reported`).
    let is_worker_error = e.class == "WorkerError";
    if log_traceback && !is_worker_error {
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
    if !log_traceback && !is_worker_error {
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

#[cfg(test)]
mod doctor_entry_tests {
    use super::*;

    #[test]
    fn main_doctor_early_returns_restore_only_owned_startup_mode() {
        let scratch = tempfile::tempdir().unwrap();
        let make_config = || {
            let mut options = ConfigOptions::with_env(vec![
                ("MRSM_ROOT_DIR".into(), scratch.path().display().to_string()),
                (
                    "MRSM_CONFIG".into(),
                    serde_json::json!({"meerschaum": {"connectors": {"sql": {"main": {
                        "flavor": "sqlite", "database": scratch.path().join("fallback.sqlite")
                    }}}}})
                    .to_string(),
                ),
            ]);
            options.allow_writes = false;
            Config::new(options).unwrap()
        };
        let mut output = Vec::new();
        let mut rt = Runtime::new(DateContext::system(), 80, &mut output, |_| None);
        rt.config = Some(make_config());
        let session = native::Session::new();
        session.set_python_worker(false);
        rt.native = Some(session.clone());
        let words = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        for args in [
            words(&["doctor", "--port", "not-a-number"]),
            words(&["doctor", "--chunk-days", "9223372036854775807"]),
            words(&["doctor", "--help"]),
            words(&["doctor", "--version"]),
            words(&["doctor", PRINT_ARG_SCHEMA_FLAG]),
        ] {
            main(&args, &mut rt);
            assert!(!session.cached_manifest_only(), "{args:?}");
            assert!(rt.doctor_startup.is_none(), "{args:?}");
            assert!(!rt.config().with_state(|s| s.allow_writes()), "{args:?}");
            assert_eq!(main(&words(&["show", "version"]), &mut rt), 0);
            assert!(!session.cached_manifest_only());
        }

        // A host-owned cached-only mode remains owned by that host. Another runtime's
        // startup marker cannot consume it or change the caller's read-only config.
        session.set_cached_manifest_only(true);
        let mut other_output = Vec::new();
        let mut other = Runtime::new(DateContext::system(), 80, &mut other_output, |_| None);
        other.config = Some(make_config());
        let other_session = native::Session::new();
        other_session.set_python_worker(false);
        other_session.set_cached_manifest_only(true);
        other.native = Some(other_session.clone());
        rt.doctor_startup = Some(false);
        main(&words(&["doctor", "--help"]), &mut other);
        assert!(other_session.cached_manifest_only());
        assert!(session.cached_manifest_only());
        assert_eq!(rt.doctor_startup, Some(false));
        assert!(!rt.config().with_state(|s| s.allow_writes()));
        assert!(!other.config().with_state(|s| s.allow_writes()));
        let config_created = rt.doctor_startup.take().unwrap();
        end_doctor_mode(&mut rt, config_created);
    }

    #[test]
    fn first_effective_doctor_respects_chains_patches_and_pipelines() {
        let dates = DateContext::system();
        let words = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        for args in [
            words(&["doctor", "--nopretty"]),
            words(&["+", "doctor", "+", "show", "version"]),
            words(&["doctor", "--not-a-valid-flag"]),
            words(&["doctor", "--port", "not-a-number"]),
            words(&["compose", "explain"]),
            words(&["compose", "explain", "--file", "x.yaml"]),
        ] {
            assert!(starts_with_doctor(&args, &dates, None).unwrap(), "{args:?}");
        }
        for args in [
            words(&["show", "version", "+", "doctor"]),
            words(&["doctor", ":", "--name", "diagnostic"]),
            words(&["compose", "up"]),
            words(&["start", "job", "diagnostic", "+", "doctor"]),
        ] {
            assert!(
                !starts_with_doctor(&args, &dates, None).unwrap(),
                "{args:?}"
            );
        }
        assert!(
            starts_with_doctor(
                &words(&["doctor", "--chunk-days", "9223372036854775807"]),
                &dates,
                None
            )
            .is_err()
        );
        let malformed = parse_arguments(&words(&["doctor", "--port", "not-a-number"]), &dates)
            .expect("invalid typed arguments are captured in the argument map");
        assert!(matches!(
            malformed.get(keys::FAILURE_KEY),
            Some(PyValue::Exception(_))
        ));
        let mut patch = ArgsMap::new();
        patch.insert("action".into(), PyValue::str_list(&words(&["doctor"])));
        assert!(starts_with_doctor(&words(&["show", "version"]), &dates, Some(&patch)).unwrap());
        patch.insert(
            "action".into(),
            PyValue::str_list(&words(&["show", "version"])),
        );
        assert!(!starts_with_doctor(&words(&["doctor"]), &dates, Some(&patch)).unwrap());
    }
}
