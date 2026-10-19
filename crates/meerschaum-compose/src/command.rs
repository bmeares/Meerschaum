//! Running Meerschaum commands inside a project: a port of
//! `meerschaum/compose/utils/__init__.py::run_mrsm_command`.
//!
//! Compose never runs anything itself: every command goes through a [`CommandRunner`], which
//! receives the exact sysargs Python would pass to `entry()` (in-process) or to
//! `python -m meerschaum` (subprocess), and the environment it would run under. `--dry` and the
//! tests use [`RecordingRunner`].

use std::path::PathBuf;

use meerschaum_core::pyfmt::{py_repr, py_str};
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::env::{EnvDict, ProcessEnv};
use crate::error::{ComposeError, Messages};
use crate::project::{Project, env_dict_as_process_env, get_env_dict, get_project_name};

/// A command compose runs in the project environment.
#[derive(Debug, Clone, PartialEq)]
pub struct MrsmCommand {
    /// The final sysargs (`mrsm <sysargs>`), after compose appended `--debug`, `--tags` and
    /// `--no-daemon`.
    pub sysargs: Vec<String>,
    /// Run as a subprocess (`python -m meerschaum`) rather than in-process (`entry()`).
    pub subprocess: bool,
    /// Capture the output (subprocess only).
    pub capture_output: bool,
    /// Run under the project's root directory, config and environment (`_replace`). In-process
    /// commands with `replace == false` run in the environment that is already active.
    pub replace: bool,
    /// The environment the command runs under when `replace` is set (`get_env_dict()`).
    pub env: Option<EnvDict>,
    /// The project's root directory when `replace` is set.
    pub root_dir: Option<PathBuf>,
    /// The project's `config:` block when `replace` is set.
    pub config: Option<Map>,
    /// The project environment (`get_env_dict()`) the subaction itself runs under. An in-process
    /// command with `replace == false` inherits it in Python (the subaction already ran
    /// `replace_env()`), so a runner that spawns a process for it must pass this environment.
    /// `None` when `replace` is set (then `env` is the environment) or when the environment
    /// cannot be applied.
    pub inherited_env: Option<EnvDict>,
}

/// Runs [`MrsmCommand`]s.
pub trait CommandRunner {
    /// Run a command and return its result. For subprocesses only success matters (Python
    /// checks the return code).
    fn run(&mut self, command: &MrsmCommand) -> SuccessTuple;

    /// Show the messages compose recorded so far, just before a command runs, so that they
    /// interleave with the command's own output as in v4 (where `info()` / `warn()` print at
    /// once). Return `true` when they were shown (compose then drops them); the default keeps
    /// them in the output's messages.
    fn show_messages(&mut self, _messages: &[crate::error::Message]) -> bool {
        false
    }
}

/// A runner that records every command and answers with a scripted result (success by default).
#[derive(Debug, Clone, Default)]
pub struct RecordingRunner {
    /// The commands, in order.
    pub commands: Vec<MrsmCommand>,
    /// Results to return, matched by a prefix of the sysargs; the first match wins.
    pub results: Vec<(Vec<String>, SuccessTuple)>,
}

impl CommandRunner for RecordingRunner {
    fn run(&mut self, command: &MrsmCommand) -> SuccessTuple {
        self.commands.push(command.clone());
        self.results
            .iter()
            .find(|(prefix, _)| command.sysargs.starts_with(prefix))
            .map(|(_, r)| r.clone())
            .unwrap_or_else(SuccessTuple::success)
    }
}

/// Options for [`run_mrsm_command`] (its keyword arguments).
#[derive(Debug, Clone, Copy)]
pub struct RunOptions {
    /// `capture_output`.
    pub capture_output: bool,
    /// `debug`.
    pub debug: bool,
    /// `_subprocess` (`None`: follow `isolation`).
    pub subprocess: Option<bool>,
    /// `_replace`.
    pub replace: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            capture_output: false,
            debug: false,
            subprocess: None,
            replace: true,
        }
    }
}

/// Build the command `run_mrsm_command(args, compose_config, ...)` would run.
pub fn build_command(
    args: &[String],
    project: &Project,
    opts: RunOptions,
    msgs: &mut Messages,
) -> Result<MrsmCommand, ComposeError> {
    build_command_with_env(args, &project.config, &project.process_env, opts, msgs)
}

/// [`build_command`] for an explicit compose config and process environment.
pub fn build_command_with_env(
    args: &[String],
    config: &Map,
    process_env: &ProcessEnv,
    opts: RunOptions,
    msgs: &mut Messages,
) -> Result<MrsmCommand, ComposeError> {
    let project_name = py_str(&get_project_name(config, msgs)?);
    let mut sysargs: Vec<String> = args.to_vec();
    if !args.iter().any(|a| a == "--debug") && opts.debug {
        sysargs.push("--debug".into());
    }
    if !args.iter().any(|a| a == "--tags" || a == "-t") && !args.join(" ").starts_with("stack ") {
        sysargs.push("--tags".into());
        sysargs.push(project_name);
    }
    if config.get("daemon") == Some(&Value::Bool(false)) {
        sysargs.push("--no-daemon".into());
    }
    let subprocess = opts
        .subprocess
        .unwrap_or_else(|| config.get("isolation").and_then(Value::as_str) == Some("subprocess"));
    // A subprocess always runs under the project environment: either it is passed explicitly
    // (`_replace`) or the subprocess inherits the environment the subaction already runs in.
    let replace = opts.replace || subprocess || opts.capture_output;

    let inherited_env = if replace {
        None
    } else {
        get_env_dict(config, process_env).ok()
    };
    let (env, root_dir, cfg) = if replace {
        let env = get_env_dict(config, process_env)?;
        // PARITY: in-process commands apply the environment with `replace_env()`, which raises
        // `TypeError` for non-string values from `environment:`.
        if !subprocess && !opts.capture_output {
            env_dict_as_process_env(&env)?;
        }
        let root = config.get("root_dir").map(|v| PathBuf::from(py_str(v)));
        let cfg = match config.get("config") {
            Some(Value::Map(m)) => m.clone(),
            _ => Map::new(),
        };
        (Some(env), root, Some(cfg))
    } else {
        (None, None, None)
    };

    Ok(MrsmCommand {
        sysargs,
        subprocess: subprocess || opts.capture_output,
        capture_output: opts.capture_output,
        replace,
        env,
        root_dir,
        config: cfg,
        inherited_env,
    })
}

/// `run_mrsm_command(args, compose_config, ...)`: build the command and run it.
pub fn run_mrsm_command(
    args: &[String],
    project: &Project,
    opts: RunOptions,
    runner: &mut dyn CommandRunner,
    msgs: &mut Messages,
) -> Result<SuccessTuple, ComposeError> {
    let command = build_command(args, project, opts, msgs)?;
    Ok(execute(&command, runner))
}

/// Run a built command, mapping a failed subprocess to Python's message.
pub fn execute(command: &MrsmCommand, runner: &mut dyn CommandRunner) -> SuccessTuple {
    let result = runner.run(command);
    if command.subprocess {
        if result.success {
            SuccessTuple::success()
        } else {
            let list = Value::List(command.sysargs.iter().cloned().map(Value::Str).collect());
            SuccessTuple::fail(format!("Failed to execute sysargs:\n{}", py_repr(&list)))
        }
    } else {
        result
    }
}

/// Convenience: string arguments.
pub fn args<I, S>(items: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    items.into_iter().map(Into::into).collect()
}
