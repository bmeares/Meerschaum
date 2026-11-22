//! The `mrsm` entry point and action dispatch, ported from `meerschaum/__main__.py` (`main`),
//! `meerschaum/_internal/entry.py` (`entry_without_daemon`, `entry_with_args`) and
//! `meerschaum/_internal/arguments/_parser.py` (`parse_help`, `parse_version`).
//!
//! Core actions resolve by the v4 naming convention and use native implementations or the
//! shared Python worker for compatibility operations.

use std::io::Write;
use std::path::PathBuf;

use meerschaum_core::config::{Config, ConfigOptions};

use crate::actions::{self, CORE_ACTIONS, Resolved};
use crate::env;
use crate::format::{FormatSettings, Formatter, TupleArgs};
use crate::help;
use crate::native;
use crate::parse::parse_arguments;
use crate::schema;
use crate::spec::keys;
use crate::types::DateContext;
use crate::value::{ArgsMap, PyException, PyValue};

pub use meerschaum_core::SuccessTuple;

mod action;
mod chain;
mod child;
mod debug_args;
mod host;

use action::{action_label, repeat, run_action};
use chain::{CHAIN_ACTION_DEF, is_chain_call, run_chain_inner, schedule_is_the_jobs};

pub use chain::run_chain;
pub use child::{
    ENTRY_CHILD_ARG, ENTRY_CHILD_ENV, PIPELINE_CHILD_ARG, entry_child, pipeline_child,
    read_entry_child_result,
};
pub use debug_args::{debug_scope, format_args_map, redacted_args};
pub use host::{RecordingHost, SystemHost};

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
    /// The program is an incidental child that is not Meerschaum (an installer, an editor, a
    /// probe): it gets no `MRSM_CONFIG`, `MRSM_PATCH`, `_FILE` form or `MRSM_<TYPE>_<LABEL>`
    /// variable ([`meerschaum_core::child_env`]). Commands the user asked for (`sh`, `os`)
    /// leave it `false`.
    pub scrub_secrets: bool,
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

    /// Stream a finite administrator command's stdout without collecting it. The
    /// default refuses, so a test double or alternate host cannot accidentally start
    /// a real Docker client. Existing captured-output behavior is unchanged.
    fn output_stream(
        &mut self,
        _spec: &CommandSpec,
        _timeout: std::time::Duration,
        _maximum_stdout: u64,
        _consume: &mut (dyn FnMut(&mut dyn std::io::Read) -> std::io::Result<()> + Send),
    ) -> std::io::Result<i32> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "This host has no bounded administrator output stream.",
        ))
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
    /// Exit the old engine after self-upgrade; the CLI launcher starts a fresh shell.
    pub restart_requested: bool,
    /// The connectors and Tokio runtime the native instance actions use (built on first use).
    pub native: Option<crate::native::Session>,
    /// Stdout is a terminal: graph runs print live progress lines before the Gantt chart. Only
    /// the process runtime sets it, so tests and piped output never see progress lines.
    pub live: bool,
    /// Where waiting feedback (spinners, [`crate::format::progress`]) is drawn. Only the process
    /// runtime turns it on (stderr is a terminal); tests and piped output never see it.
    pub progress: crate::format::progress::ProgressSink,
    /// Whether [`Runtime::out`] is a terminal, for hosts whose `out` is not the process's stdout
    /// (Python's `sys.stdout`, where a notebook counts as a terminal). `None`: stdout's own check.
    pub out_terminal: Option<bool>,
    /// Whether stderr is a terminal (only the process runtime sets it): warnings and debug output
    /// are styled by stderr's own check, so `2> debug.log` gets no escape sequences. `None`
    /// (tests, embedded hosts): stderr follows the stdout settings.
    pub err_terminal: Option<bool>,
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
            restart_requested: false,
            native: None,
            live: std::io::IsTerminal::is_terminal(&std::io::stdout()),
            progress: crate::format::progress::ProgressSink::stderr_if_tty(),
            out_terminal: None,
            err_terminal: Some(std::io::IsTerminal::is_terminal(&std::io::stderr())),
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
            restart_requested: false,
            native: None,
            live: false,
            progress: crate::format::progress::ProgressSink::Off,
            out_terminal: None,
            err_terminal: None,
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
                // PARITY: `config/_paths.py` prints this and exits 1 for a missing
                // `MRSM_ROOT_DIR` (and the other `MRSM_*` path variables).
                if let ConfigError::InvalidPath { var, path } = &e {
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
        match self.out_terminal {
            Some(terminal) => FormatSettings::detect_with(self.config(), env, terminal),
            None => FormatSettings::detect(self.config(), env),
        }
    }

    /// The formatting settings of text written to stderr: the stdout settings, except that the
    /// process runtime decides styles by stderr's own terminal check ([`Runtime::err_terminal`]).
    pub fn err_settings(&mut self) -> FormatSettings {
        match (&self.format, self.err_terminal) {
            (None, Some(terminal)) => {
                let env = self.env;
                FormatSettings::detect_with(self.config(), env, terminal)
            }
            _ => self.settings(),
        }
    }

    /// Run a closure with a [`Formatter`] for text written to stderr ([`Runtime::err_settings`]).
    pub fn with_err_formatter<R>(&mut self, f: impl FnOnce(&Formatter<'_>) -> R) -> R {
        let settings = self.err_settings();
        let config = self.config();
        let formatter = Formatter::new(settings, config);
        f(&formatter)
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

    /// Write to stderr (without SGR sequences when stderr takes no styles, see
    /// [`Runtime::err_settings`]).
    pub fn eprint(&mut self, text: &str) {
        crate::shell::interrupt::check();
        crate::format::progress::suspend();
        crate::native::api_warnings::flush(self);
        let plain = self.format.is_none()
            && self.err_terminal.is_some()
            && text.contains('\x1b')
            && !self.err_settings().ansi;
        let text = if plain {
            std::borrow::Cow::Owned(crate::format::remove_ansi(text))
        } else {
            std::borrow::Cow::Borrowed(text)
        };
        let _ = self.err.write_all(text.as_bytes());
        let _ = self.err.flush();
        crate::format::progress::resume(text.is_empty() || text.ends_with('\n'));
    }
}

fn contains(sysargs: &[String], flag: &str) -> bool {
    // The words after the file of `python file.py ...` are the script's (`-h` is its own).
    crate::parse::meerschaum_words(sysargs)
        .iter()
        .take_while(|a| !matches!(a.as_str(), "--" | "-A" | "--sub-args"))
        .any(|a| a == flag)
}

/// `meerschaum.__main__.main`: returns the process exit code.
pub fn main(sysargs: &[String], rt: &mut Runtime<'_>) -> i32 {
    // `doctor` is read-only: from the first config read on, nothing is written and the plugin
    // manifest is the cached one (never validated or rebuilt, which imports plugin code).
    let doctor = starts_with_doctor(sysargs, &rt.dates, None).unwrap_or(true);
    if !doctor && rt.config.is_none() {
        // A root created by an earlier 5.0 release candidate keeps pandas DataFrames: `dataframe: pandas` is
        // added to its `meerschaum.yaml` before anything reads its config.
        use meerschaum_core::config::dataframe::{PinOutcome, pin_process_root};
        if let Some(PinOutcome::Pinned { notice } | PinOutcome::Failed { notice }) =
            pin_process_root()
        {
            eprintln!("{notice}");
        }
    }
    if doctor {
        let config_created = rt.config.is_none();
        rt.config_read_only();
        let session = native::session(rt);
        if !session.cached_manifest_only() {
            session.set_cached_manifest_only(true);
            rt.doctor_startup = Some(config_created);
        }
    }
    let debug = debug_scope(rt, contains(sysargs, "--debug"));
    let code = main_inner(sysargs, rt);
    drop(debug);
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
    if contains(sysargs, "--help")
        || contains(sysargs, "-h")
        || sysargs.first().map(String::as_str) == Some("help")
    {
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
    let in_job = (rt.env)(meerschaum_jobs::supervisor::RESULT_PATH_ENV).is_some();
    let manifest_error = if starts_with_executor(sysargs, &rt.dates, None).unwrap_or(true) {
        None
    } else if starts_interactive_shell(sysargs, &rt.dates, rt.env, in_job) {
        // An interactive shell or webterm opens immediately on cached plugin metadata and
        // refreshes stale plugin manifests quietly in the background.
        let session = native::session(rt);
        session.warm_manifest_for_shell(rt.config());
        None
    } else {
        let progress = crate::format::progress::Progress::start_flags(
            rt,
            contains(sysargs, "--nopretty"),
            contains(sysargs, "--debug"),
            "Loading plugins",
        );
        let manifest = crate::python::manifest(rt);
        drop(progress);
        manifest.err().map(|e| e.message)
    };
    let nopretty = contains(sysargs, "--nopretty");
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
    // always exited 0 (`meerschaum/__main__.py` `main`). v5 exits 1 on failure
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

/// Whether the first effective step starts an executor. Bootstrap uses core argument parsing
/// only: persisted team plugins must not load before a fresh signed configuration arrives.
pub fn starts_with_executor(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
) -> Result<bool, PyException> {
    let mut executor = false;
    run_chain_inner(sysargs, dates, patch_args, false, |args| {
        executor = is_executor_action(&action_words(&args));
        SuccessTuple::fail("")
    })?;
    Ok(executor)
}

fn is_executor_action(action: &[String]) -> bool {
    matches!(action, [first, second, ..] if first == "start" && second == "executor")
}

/// Whether `sysargs` opens an interactive shell prompt with no immediate action to run (such as
/// `mrsm`, `mrsm --shell`, or webterm's `mrsm -i <instance>`). Piped stdin and jobs keep
/// synchronous manifest validation so commands fed on stdin see fresh plugin metadata at once.
fn starts_interactive_shell(
    sysargs: &[String],
    dates: &DateContext,
    env: fn(&str) -> Option<String>,
    in_job: bool,
) -> bool {
    !in_job
        && !crate::env::noninteractive(env)
        && std::io::IsTerminal::is_terminal(&std::io::stdin())
        && is_empty_action_shell_invocation(sysargs, dates)
}

fn is_empty_action_shell_invocation(sysargs: &[String], dates: &DateContext) -> bool {
    if contains(sysargs, "-d")
        || contains(sysargs, "--daemon")
        || contains(sysargs, keys::AND)
        || contains(sysargs, keys::PIPELINE)
    {
        return false;
    }
    if sysargs.is_empty() {
        return true;
    }
    let filtered: Vec<String> = sysargs
        .iter()
        .filter(|a| *a != "--shell")
        .cloned()
        .collect();
    if filtered.is_empty() {
        return true;
    }
    parse_arguments(&filtered, dates).is_ok_and(|args| {
        let action = action_words(&args);
        action.is_empty() || (action.len() == 1 && action[0] == "mrsm")
    })
}

struct ExecutorBootstrapManifest {
    session: native::Session,
    was_cached: bool,
}
impl Drop for ExecutorBootstrapManifest {
    fn drop(&mut self) {
        self.session.set_cached_manifest_only(self.was_cached);
    }
}

/// What the dispatcher runs for the action words `action`: a native plugin's action, a
/// Python plugin's action, a core action, or, for words that name none, the system shell (v4's
/// fallthrough to `sh`). The dispatcher decides with this function, and the API server asks it
/// for each step of a non-administrator's command line ([`resolve_sysargs`]), so the server
/// refuses exactly what would reach the shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionTarget {
    /// A native plugin's action (`plugin`, `name`); it shadows a Python plugin's.
    NativePlugin {
        /// The plugin.
        plugin: String,
        /// The action.
        name: String,
    },
    /// A Python plugin's action.
    PythonPlugin {
        /// The plugin that declares it (`None` when the manifest names none).
        plugin: Option<String>,
        /// The action.
        name: String,
    },
    /// A core action.
    Core(Resolved),
    /// No action: the words run as a shell command.
    Shell,
}

/// Resolve `action` as the dispatcher does: native plugin actions (when `natives` is known),
/// then every plugin action of `manifest`, then the core actions.
pub fn resolve_target(
    action: &[String],
    manifest: Option<&meerschaum_worker::manifest::Manifest>,
    natives: Option<&meerschaum_plugin::registry::NativePlugins>,
) -> ActionTarget {
    if let Some(manifest) = manifest {
        if let Some(natives) = natives
            && let Some((plugin, name)) =
                crate::native_plugins::action_owner(action, manifest, natives)
        {
            return ActionTarget::NativePlugin { plugin, name };
        }
        if let Some(name) = crate::python::action_name(action, manifest) {
            let plugin = manifest
                .plugins
                .values()
                .find(|p| p.actions.iter().any(|a| a.name == name))
                .map(|p| p.name.clone());
            return ActionTarget::PythonPlugin { plugin, name };
        }
    }
    match actions::get_action(action, CORE_ACTIONS) {
        Some(resolved) => ActionTarget::Core(resolved),
        None => ActionTarget::Shell,
    }
}

/// What one step of a command line (`sysargs`, without `+` or `:`) resolves to, parsed and
/// resolved as the dispatcher would: the real argument parser, the plugins' own flags, then
/// [`resolve_target`]. A step the parser rejects runs nothing; its leading words are resolved.
pub fn resolve_sysargs(sysargs: &[String], rt: &mut Runtime<'_>) -> ActionTarget {
    let dates = rt.dates.clone();
    let mut args = match parse_arguments(sysargs, &dates) {
        Ok(args) => args,
        Err(_) => {
            let leading: Vec<String> = sysargs
                .iter()
                .take_while(|a| !a.starts_with('-'))
                .cloned()
                .collect();
            return resolve_target(&leading, None, None);
        }
    };
    let manifest = crate::python::manifest(rt).ok();
    if let Some(manifest) = manifest.as_deref() {
        let _ = crate::python::arguments(&mut args, manifest, rt);
    }
    let action = action_words(&args);
    if action.is_empty() {
        // No action opens the interactive shell (a job refuses it).
        return ActionTarget::Shell;
    }
    let natives = manifest
        .as_ref()
        .and_then(|_| native::session(rt).native_plugins(rt.config()).ok());
    resolve_target(&action, manifest.as_deref(), natives.as_deref())
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

    let captured_graph = match native::graph_plan::captured_dispatch(rt, &action, &sysargs) {
        Ok(Some(authority)) => {
            if let [command, subject, name] = action.as_slice()
                && command == "sync"
                && subject == "pipelines"
            {
                return native::pipelines::run_captured(&args, rt, name, authority)
                    .unwrap_or_else(|error| SuccessTuple::fail(error.message));
            }
            true
        }
        Ok(None) => false,
        Err(error) => return SuccessTuple::fail(error),
    };
    // Main authorizes these forms as built-in commands on the API-only profile. A plugin
    // action with the same name cannot substitute a different operation at dispatch.
    let bound_command =
        (rt.env)(meerschaum_core::team_scope::api_execution::PROFILE_VAR).as_deref() == Some("1")
            && meerschaum_server::execute_policy::bound_command_scopes(&sysargs).is_some();
    let bootstrap = is_executor_action(&action);
    let _bootstrap_manifest = bootstrap.then(|| {
        let session = native::session(rt);
        let was_cached = session.cached_manifest_only();
        session.set_cached_manifest_only(true);
        ExecutorBootstrapManifest {
            session,
            was_cached,
        }
    });
    if !bootstrap
        && !captured_graph
        && !bound_command
        && let Ok(manifest) = crate::python::manifest(rt)
        && let Err(e) = crate::python::arguments(&mut args, &manifest, rt)
    {
        return SuccessTuple::fail(format!("Invalid arguments:\n    {}", e.message));
    }
    let _debug = debug_scope(rt, args.get("debug").is_some_and(PyValue::is_truthy));
    // `--allow-protected`: writes to `protected: true` connectors are allowed while this action
    // runs (the core write paths check `meerschaum_core::protected::allowed()`; Python workers
    // get the allowance with each call). `-y` never counts.
    let _allow_protected = meerschaum_core::protected::allow(
        args.get("allow_protected").is_some_and(PyValue::is_truthy),
    );
    meerschaum_core::dprint!("Parsed arguments:\n{}", redacted_args(&args));
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
    let daemon = args.get("daemon").is_some_and(PyValue::is_truthy);
    if daemon && action[0] != "stack" {
        action.splice(0..0, ["start".to_string(), "jobs".to_string()]);
    }
    // A serialized action chain (`chain <flags> -P '{"sub_args_line": ...}'`): the internal
    // action is never a plugin's and never listed (see `meerschaum_core::chain`).
    let chain_call = !daemon && is_chain_call(&action, &args);
    // The words that name the action, for messages (`show pipes`).
    let label = action_label(&action);
    // What runs: a plugin's action, a core action, or the shell ([`resolve_target`], the one
    // resolution the server's checks for non-administrators also use).
    let (target, manifest, natives) =
        if chain_call || daemon || bootstrap || captured_graph || bound_command {
            (resolve_target(&action, None, None), None, None)
        } else {
            match crate::python::manifest(rt) {
                Ok(manifest) => {
                    let natives = native::session(rt).native_plugins(rt.config()).ok();
                    (
                        resolve_target(&action, Some(&manifest), natives.as_deref()),
                        Some(manifest),
                        natives,
                    )
                }
                Err(_) => (resolve_target(&action, None, None), None, None),
            }
        };
    if let ActionTarget::NativePlugin { plugin, name } = &target
        && let Some(natives) = natives
    {
        // Native (C-ABI) plugin actions run in-process; they shadow Python plugin actions.
        let (plugin, name) = (plugin.clone(), name.clone());
        meerschaum_core::dprint!("Action {action:?} -> native plugin action '{name}'.");
        env::apply_env_arguments(&mut args, rt.env);
        args.insert("action".into(), PyValue::str_list(&action));
        let noun = native::jobs_laps::lap_noun(&action);
        return repeat(rt, &args, skip_schedule, true, &label, noun, |args, rt| {
            crate::native_plugins::run_action_in(rt, &plugin, &name, args, &natives)
                .unwrap_or_else(|e| SuccessTuple::fail(format!("{}: {}", e.class, e.message)))
        });
    }
    if let ActionTarget::PythonPlugin { .. } = &target
        && let Some(manifest) = manifest
    {
        meerschaum_core::dprint!("Action {action:?} -> Python plugin action.");
        env::apply_env_arguments(&mut args, rt.env);
        let noun = native::jobs_laps::lap_noun(&action);
        // An action whose function takes `loop` repeats itself with `--loop`, as in 4.1: the
        // host does not run it in laps.
        let loops = !crate::python::action_handles_loop(&action, &manifest);
        return repeat(rt, &args, skip_schedule, loops, &label, noun, |args, rt| {
            // The terminal gives the worker the Ctrl-C (its call raises `KeyboardInterrupt`
            // and returns); a second one within two seconds drops the call, which kills the
            // worker.
            let _waiting = crate::shell::interrupt::ChildScope::enter(None, true);
            crate::python::plugin_action(args, rt)
                .unwrap_or_else(|e| SuccessTuple::fail(format!("{}: {}", e.class, e.message)))
        });
    }
    let resolved: Resolved = if chain_call {
        Resolved {
            action: &CHAIN_ACTION_DEF,
            sub: None,
        }
    } else {
        match target {
            ActionTarget::Core(r) => r,
            // A restricted terminal never falls through to the shell, whatever it was told.
            _ if crate::shell::restricted::active() => {
                return SuccessTuple::fail(format!(
                    "'{label}' is not a Meerschaum action. {}",
                    crate::shell::restricted::NO_PROGRAMS
                ));
            }
            _ => {
                // Unknown actions run as shell commands in v4.
                action.insert(0, "sh".into());
                match actions::get_action(&["sh"], CORE_ACTIONS) {
                    Some(r) => r,
                    None => return SuccessTuple::fail("action `sh` is not available"),
                }
            }
        }
    };
    if action[0] == "start" && action.get(1).is_some_and(|a| a == "job" || a == "jobs") {
        skip_schedule = true;
    }
    if schedule_is_the_jobs(resolved.function_name(), &args) {
        skip_schedule = true;
    }
    let remaining = if chain_call {
        action[1..].to_vec()
    } else {
        actions::remove_leading_action(&action, CORE_ACTIONS)
    };
    meerschaum_core::dprint!(
        "{}",
        meerschaum_core::debug::redact_uris(&format!(
            "Action {action:?} -> `{}` ({}), sub-actions {remaining:?}.",
            resolved.display_name(),
            resolved.function_name()
        ))
    );
    args.insert("action".into(), PyValue::str_list(&remaining));
    env::apply_env_arguments(&mut args, rt.env);

    let loops = !native::jobs_laps::handles_own_loop(resolved.function_name());
    let noun = native::jobs_laps::lap_noun(&action);
    repeat(rt, &args, skip_schedule, loops, &label, noun, |args, rt| {
        run_action(&resolved, args, rt, depth)
    })
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

/// `parse_help(sysargs)`: choose which help message to print.
pub fn parse_help(sysargs: &[String], rt: &mut Runtime<'_>) {
    let args = parse_arguments(sysargs, &rt.dates).unwrap_or_default();
    let mut action = action_words(&args);
    // `mrsm help sync pipes` is `mrsm sync pipes --help`.
    if action.first().map(String::as_str) == Some("help") {
        action.remove(0);
    }
    help::print_help(&action, &args, rt);
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
mod debug_tests {
    use super::*;

    #[test]
    fn redacted_args_hide_secrets() {
        let mut args = ArgsMap::new();
        args.insert("action".into(), PyValue::str_list(&["login".to_string()]));
        args.insert("password".into(), PyValue::Str("hunter2".into()));
        args.insert(
            "sysargs".into(),
            PyValue::str_list(&[
                "login".to_string(),
                "--password".to_string(),
                "hunter2".to_string(),
                "--api-key=hunter2".to_string(),
                "--client-secret".to_string(),
                "hunter2".to_string(),
            ]),
        );
        args.insert(
            "uri".into(),
            PyValue::Str("postgresql://user:hunter2@localhost/db".into()),
        );
        args.insert(
            "params".into(),
            PyValue::Dict(vec![
                (PyValue::str("password"), PyValue::str("hunter2")),
                (
                    PyValue::str("fetch"),
                    PyValue::Dict(vec![
                        (
                            PyValue::str("uri"),
                            PyValue::str("postgresql://h/db?password=hunter2"),
                        ),
                        (PyValue::str("client_secret"), PyValue::str("hunter2")),
                    ]),
                ),
            ]),
        );
        args.insert(
            "filtered_sysargs".into(),
            PyValue::str_list(&[
                "--params".to_string(),
                r#"{"password": "hunter2"}"#.to_string(),
                "--sub-args".to_string(),
                "--password hunter2".to_string(),
            ]),
        );
        args.insert(
            "sub_args".into(),
            PyValue::str_list(&[
                "-P".to_string(),
                r#"{"token":123456789,"api_key":["live-secret"]}"#.to_string(),
                "token:987654321,id:1".to_string(),
                "mysql://u:hunter2@h/db".to_string(),
            ]),
        );
        let text = redacted_args(&args);
        for secret in ["hunter2", "123456789", "987654321", "live-secret"] {
            assert!(!text.contains(secret), "{text}");
        }
        assert!(text.contains("'fetch'"), "{text}");
        assert!(text.contains("'login'"), "{text}");
        assert!(text.contains("--api-key=***"), "{text}");
        assert!(
            text.contains("postgresql://user:***@localhost/db"),
            "{text}"
        );
    }
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

#[cfg(test)]
mod target_tests {
    use super::*;

    fn w(words: &[&str]) -> Vec<String> {
        words.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn words_that_name_no_action_resolve_to_the_shell() {
        for words in [
            &["touch", "/x"][..],
            &["!touch", "/x"],
            &["mrsm", "show"],
            &[""],
        ] {
            assert_eq!(resolve_target(&w(words), None, None), ActionTarget::Shell);
        }
        for (words, name) in [
            (&["show", "pipes"][..], "show"),
            (&["sh", "ls"], "sh"),
            (&["sync", "pipes"], "sync"),
        ] {
            match resolve_target(&w(words), None, None) {
                ActionTarget::Core(resolved) => assert_eq!(resolved.action.name, name),
                other => panic!("{words:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn empty_action_shell_invocations_match_webterm_and_plain_shell() {
        let dates = DateContext::system();
        for yes in [
            &[][..],
            &["--shell"],
            &["-i", "sql:sra"],
            &["-i", "sql:sra", "--shell"],
            &["--mrsm-instance", "sql:sra", "--debug"],
        ] {
            assert!(
                is_empty_action_shell_invocation(&w(yes), &dates),
                "expected shell invocation for {yes:?}"
            );
        }
        for no in [
            &["show", "pipes"][..],
            &["--shell", "show", "pipes"],
            &["-d", "-i", "sql:sra"],
            &["-i", "sql:sra", "+", "show", "version"],
            &["-i", "sql:sra", ":", "-w", "1"],
        ] {
            assert!(
                !is_empty_action_shell_invocation(&w(no), &dates),
                "expected non-shell invocation for {no:?}"
            );
        }
    }
}
