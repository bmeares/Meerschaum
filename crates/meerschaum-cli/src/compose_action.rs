//! `mrsm compose ...` wired to `meerschaum-compose` (`meerschaum/actions/compose.py`).
//!
//! The commands compose runs re-invoke this binary ([`MrsmRunner`]) under the project
//! environment. Pipe registrations and jobs need an instance connector and the job supervisor,
//! which the CLI does not link yet, so only `explain`, `init` and `up --dry` run; the other
//! subactions return the not-implemented tuple.

use std::cell::RefCell;
use std::path::PathBuf;

use meerschaum_compose::command::{CommandRunner, MrsmCommand};
use meerschaum_compose::env::env_dict_to_strings;
use meerschaum_compose::error::Message;
use meerschaum_compose::ops::{InMemoryInstance, InMemoryJobs, Prompter};
use meerschaum_compose::project::{Host, Session};
use meerschaum_compose::subactions::{ComposeArgs, Services, compose};
use meerschaum_core::SuccessTuple;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::{CommandSpec, Runtime};
use crate::value::{ArgsMap, PyValue};

/// Runs compose's commands by re-invoking `mrsm` (the current executable).
pub struct MrsmRunner<'r, 'a> {
    rt: &'r RefCell<&'r mut Runtime<'a>>,
}

impl CommandRunner for MrsmRunner<'_, '_> {
    fn run(&mut self, command: &MrsmCommand) -> SuccessTuple {
        let program = std::env::current_exe()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "mrsm".into());
        let (env, env_clear) = match &command.env {
            Some(env) => (env_dict_to_strings(env), true),
            None => (Vec::new(), false),
        };
        let spec = CommandSpec {
            program,
            args: command.sysargs.clone(),
            env,
            env_clear,
            quiet: command.capture_output,
            cwd: None,
        };
        match self.rt.borrow_mut().host.run(&spec) {
            Ok(0) => SuccessTuple::success(),
            Ok(code) => SuccessTuple::fail(format!("Command returned exit code {code}.")),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }
}

/// Answers compose's yes/no questions from the runtime's line reader.
struct HostPrompter<'r, 'a> {
    rt: &'r RefCell<&'r mut Runtime<'a>>,
}

impl Prompter for HostPrompter<'_, '_> {
    fn ask(&mut self, question: &str, default_yes: bool) -> Option<bool> {
        let suffix = if default_yes { "[Y/n]" } else { "[y/N]" };
        let answer = self
            .rt
            .borrow_mut()
            .host
            .read_line(&format!(" ❓ {question} {suffix} "))?;
        let a = answer.trim().to_lowercase();
        Some(if a.is_empty() {
            default_yes
        } else {
            a.starts_with('y')
        })
    }
}

fn flag(args: &ArgsMap, key: &str) -> bool {
    args.get(key).is_some_and(PyValue::is_truthy)
}

fn path(args: &ArgsMap, key: &str) -> Option<PathBuf> {
    match args.get(key) {
        None | Some(PyValue::None) => None,
        Some(v) => Some(PathBuf::from(v.py_str())),
    }
}

/// `compose(action, file, env_file, ...)`.
pub fn compose_action(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action: Vec<String> = args
        .get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    let sysargs: Vec<String> = args
        .get("sysargs")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    let dry = flag(args, "dry");
    let sub = action.first().map(String::as_str).unwrap_or("default");
    let supported = matches!(sub, "explain" | "init") || (sub == "up" && dry);
    if !supported {
        return crate::python::core_action("compose", args, rt);
    }
    let settings = rt.settings();
    let compose_args = ComposeArgs {
        action: action.clone(),
        sysargs,
        file: path(args, "file"),
        env_file: path(args, "env_file"),
        dry,
        force: flag(args, "force"),
        presync: flag(args, "presync"),
        no_jobs: flag(args, "no_jobs"),
        isolated: flag(args, "isolated"),
        drop: flag(args, "drop"),
        yes: flag(args, "yes"),
        noask: flag(args, "noask"),
        nopretty: flag(args, "nopretty"),
        debug: flag(args, "debug"),
        width: Some(settings.width),
    };
    let cwd = rt
        .cwd
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    let env = (rt.vars)().into_iter().collect();
    let host =
        Host::new(cwd, env).map_err(|e| ActionError::new(e.kind.python_name(), e.message))?;
    let mut session = Session::default();
    let mut instance = InMemoryInstance::default();
    let mut jobs = InMemoryJobs::default();
    let output = {
        let shared = RefCell::new(&mut *rt);
        let mut runner = MrsmRunner { rt: &shared };
        let mut prompter = HostPrompter { rt: &shared };
        compose(
            &host,
            &mut session,
            &compose_args,
            Services {
                runner: &mut runner,
                instance: &mut instance,
                jobs: &mut jobs,
                prompter: &mut prompter,
            },
        )
    };
    for m in &output.messages.items {
        match m {
            Message::Warn(w) => {
                let text = rt.with_formatter(|f| f.warn(w));
                rt.eprint(&text);
            }
            Message::Info(i) => {
                let text = rt.with_formatter(|f| f.info(i, true));
                rt.print(&text);
            }
        }
    }
    if let Some(explain) = &output.explain {
        rt.print(explain);
        if !explain.ends_with('\n') {
            rt.print("\n");
        }
    }
    // Rust-only: the `explain` lint (v4 has none) goes to stderr as warnings.
    for finding in &output.lint {
        let text = rt.with_formatter(|f| f.warn(&finding.to_string()));
        rt.eprint(&text);
    }
    output
        .result
        .map_err(|e| ActionError::new(e.kind.python_name(), e.message))
}
