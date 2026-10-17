//! `mrsm compose ...` wired to `meerschaum-compose` (`meerschaum/actions/compose.py`).
//!
//! The commands compose runs re-invoke this binary ([`MrsmRunner`]) under the project
//! environment. Pipe registrations are read from the project's real instance connectors
//! ([`ComposeInstance`]); job discovery uses the native supervisor under the project config.

use std::cell::RefCell;
use std::path::PathBuf;

use meerschaum_compose::command::{CommandRunner, MrsmCommand};
use meerschaum_compose::env::env_dict_to_strings;
use meerschaum_compose::error::Message;
use meerschaum_compose::ops::{JobInfo, JobOps, Prompter};
use meerschaum_compose::pipes::ProjectConfig;
use meerschaum_compose::project::{Host, Session};
use meerschaum_compose::subactions::{ComposeArgs, ComposeOutput, Services, compose};
use meerschaum_core::SuccessTuple;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::{CommandSpec, Runtime};
use crate::native::compose_instance::ComposeInstance;
use crate::value::{ArgsMap, PyValue};

/// Compose's view of the job executors, through `meerschaum-jobs`.
///
/// Job discovery, ownership, `ps`, `logs`, replacement and `down` all list jobs on the
/// *project's* executor: `meerschaum:executor` of the project's `config:` block or environment
/// (an `api:<label>` shared by several projects), else `systemd` on a systemd host running the
/// default root, else `local` (`get_jobs()` with no executor keys). The commands compose then
/// runs (`delete job`, `start job`, ...) are child `mrsm` processes under the same project
/// environment, so they select the same executor.
#[derive(Default)]
pub struct ComposeJobs {
    context: Option<meerschaum_jobs::JobsContext>,
    session: Option<crate::native::Session>,
    process: Option<std::sync::Arc<dyn meerschaum_jobs::ProcessRunner>>,
    warnings: Vec<String>,
}

impl ComposeJobs {
    /// Use this process runner for `systemd` jobs (tests pass a fake `systemctl`).
    pub fn with_process_runner(
        mut self,
        runner: std::sync::Arc<dyn meerschaum_jobs::ProcessRunner>,
    ) -> ComposeJobs {
        self.process = Some(runner);
        self
    }

    /// The project's jobs context (`None` before [`JobOps::configure`]).
    pub fn context(&self) -> Option<&meerschaum_jobs::JobsContext> {
        self.context.as_ref()
    }
}

impl JobOps for ComposeJobs {
    fn configure(
        &mut self,
        host: &Host,
        project: &meerschaum_compose::Project,
    ) -> Result<(), String> {
        self.context = None;
        let project = ProjectConfig::new(host, project).map_err(|e| e.message)?;
        let config = project.registry();
        let session = self
            .session
            .get_or_insert_with(crate::native::Session::new)
            .clone();
        let resolver = crate::native::remote_jobs::CliRemoteResolver::new(
            session,
            crate::native::remote_jobs::CliRemoteResolver::section(config),
        );
        let mut context = meerschaum_jobs::JobsContext::new(
            meerschaum_jobs::config::JobPaths::from_config(config),
            meerschaum_jobs::config::JobsConfig::from_config(config),
            meerschaum_jobs::Launcher::from_env(),
        )
        .with_remote(std::sync::Arc::new(resolver));
        if let Some(runner) = &self.process {
            context = context.with_process_runner(runner.clone());
        }
        self.context = Some(context);
        Ok(())
    }

    fn get_jobs(&mut self) -> indexmap::IndexMap<String, JobInfo> {
        self.try_get_jobs().unwrap_or_default()
    }

    fn try_get_jobs(&mut self) -> Result<indexmap::IndexMap<String, JobInfo>, String> {
        let Some(context) = &self.context else {
            return Ok(indexmap::IndexMap::new());
        };
        // `get_jobs()` with no executor keys: the project's executor, combining `local` and
        // `systemd` on a systemd host; hidden jobs excluded.
        let listing = meerschaum_jobs::executor::get_jobs_for(context, None, false, true)?;
        self.warnings.extend(listing.warnings);
        Ok(listing
            .jobs
            .into_iter()
            .map(|(name, mut job)| {
                let info = JobInfo {
                    sysargs: job.sysargs(),
                    env: job.env(),
                    status: job.status().as_str().into(),
                    restart: job.restarts(),
                };
                (name, info)
            })
            .collect())
    }

    fn take_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.warnings)
    }
}

/// Runs compose's commands by re-invoking `mrsm` (the current executable).
pub struct MrsmRunner<'r, 'a> {
    rt: &'r RefCell<&'r mut Runtime<'a>>,
}

impl CommandRunner for MrsmRunner<'_, '_> {
    fn run(&mut self, command: &MrsmCommand) -> SuccessTuple {
        let program = std::env::current_exe()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "mrsm".into());
        // An in-process command (`replace == false`) runs inside the subaction's project
        // environment in Python, so the child gets that environment too.
        let (env, env_clear) = match command.env.as_ref().or(command.inherited_env.as_ref()) {
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

/// Run compose over the project's native instance connectors and supervisor, with commands
/// going through `runner` under the project environment.
pub fn compose_native(
    host: &Host,
    args: &ComposeArgs,
    runner: &mut dyn CommandRunner,
    prompter: &mut dyn Prompter,
) -> ComposeOutput {
    compose_native_with_jobs(host, args, runner, prompter, &mut ComposeJobs::default())
}

/// [`compose_native`] over a caller-built [`ComposeJobs`] (tests substitute the `systemctl`
/// runner of `systemd` jobs).
pub fn compose_native_with_jobs(
    host: &Host,
    args: &ComposeArgs,
    runner: &mut dyn CommandRunner,
    prompter: &mut dyn Prompter,
    jobs: &mut ComposeJobs,
) -> ComposeOutput {
    let mut session = Session::default();
    let mut instance = ComposeInstance::new();
    compose(
        host,
        &mut session,
        args,
        Services {
            runner,
            instance: &mut instance,
            jobs,
            prompter,
        },
    )
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
    let output = {
        let shared = RefCell::new(&mut *rt);
        let mut runner = MrsmRunner { rt: &shared };
        let mut prompter = HostPrompter { rt: &shared };
        compose_native(&host, &compose_args, &mut runner, &mut prompter)
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
