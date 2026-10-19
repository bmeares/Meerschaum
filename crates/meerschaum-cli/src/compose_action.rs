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

/// Runs compose's commands by re-invoking `mrsm` (the current executable), except the
/// in-process `register pipes` commands, which run in this process ([`MrsmRunner::run_in_process`]).
pub struct MrsmRunner<'r, 'a> {
    rt: &'r RefCell<&'r mut Runtime<'a>>,
    /// The project view in-process commands run under, by project environment: its config
    /// registry and a session whose Python workers get that environment (one worker pool for
    /// every registration of the project instead of one worker per `mrsm` child).
    projects: Vec<ProjectView>,
}

/// One project environment's config registry (lent to the runtime while a command runs) and
/// session.
struct ProjectView {
    env: Vec<(String, String)>,
    config: Option<meerschaum_core::config::Config>,
    session: crate::native::Session,
}

impl<'r, 'a> MrsmRunner<'r, 'a> {
    fn new(rt: &'r RefCell<&'r mut Runtime<'a>>) -> Self {
        MrsmRunner {
            rt,
            projects: Vec::new(),
        }
    }

    /// Whether `command` runs in this process: an in-process (`_subprocess=False`) `register
    /// pipes`, the command compose runs once per newly registered pipe.
    ///
    /// v4 runs in-process commands with `entry(sysargs)` inside the subaction's project
    /// environment: the result is returned, never printed. A child `mrsm` printed
    /// "Successfully registered 1 pipe." per pipe and started a Python worker per pipe for the
    /// connector's `register()` hook (`compose up` on a large file took several times v4's time).
    /// Other in-process commands still run as children: they may start jobs, whose environment
    /// must be the project's, which only a child process has.
    fn in_process(command: &MrsmCommand) -> bool {
        !command.subprocess
            && !command.capture_output
            && command.sysargs.first().map(String::as_str) == Some("register")
            && matches!(
                command.sysargs.get(1).map(String::as_str),
                Some("pipes" | "pipe")
            )
    }

    /// Run `command` with `entry(sysargs)` under the project's config and worker environment
    /// `env`, returning its result without printing it (v4's `run_mrsm_command(...,
    /// _subprocess=False)`).
    fn run_in_process(
        &mut self,
        command: &MrsmCommand,
        env: Vec<(String, String)>,
    ) -> SuccessTuple {
        let index = match self.projects.iter().position(|p| p.env == env) {
            Some(index) => index,
            None => {
                let config = match meerschaum_core::config::Config::new(
                    meerschaum_core::config::ConfigOptions::with_env(env.clone()),
                ) {
                    Ok(config) => config,
                    Err(e) => return SuccessTuple::fail(e.to_string()),
                };
                let session = crate::native::Session::new();
                if let Err(e) = session.set_worker_environment(env.clone()) {
                    return SuccessTuple::fail(e.message);
                }
                self.projects.push(ProjectView {
                    env,
                    config: Some(config),
                    session,
                });
                self.projects.len() - 1
            }
        };
        let project = &mut self.projects[index];
        let mut guard = self.rt.borrow_mut();
        let rt: &mut Runtime<'_> = &mut guard;
        let host_config = std::mem::replace(&mut rt.config, project.config.take());
        let host_session = rt.native.replace(project.session.clone());
        let result = crate::dispatch::entry(&command.sysargs, rt);
        project.config = std::mem::replace(&mut rt.config, host_config);
        rt.native = host_session;
        result
    }
}

impl Drop for MrsmRunner<'_, '_> {
    fn drop(&mut self) {
        for project in &self.projects {
            project.session.shutdown();
        }
    }
}

impl CommandRunner for MrsmRunner<'_, '_> {
    fn show_messages(&mut self, messages: &[Message]) -> bool {
        print_messages(&mut self.rt.borrow_mut(), messages);
        true
    }

    fn run(&mut self, command: &MrsmCommand) -> SuccessTuple {
        if Self::in_process(command)
            && let Some(env) = command.env.as_ref().or(command.inherited_env.as_ref())
        {
            return self.run_in_process(command, env_dict_to_strings(env));
        }
        // This program again: the `mrsm` binary, or `python -m meerschaum` in the extension.
        let (program, mut leading) = meerschaum_core::self_exe::self_program();
        // An in-process command returns its result instead of printing it (v4's `entry()`):
        // the child writes it to a file in a private directory this process created, named on
        // its command line and in `ENTRY_CHILD_ENV` (only a child of this runner honors it).
        // Without a private directory with a UTF-8 path (the command line and the environment
        // carry it as text), the command runs as a plain child and only its exit code counts.
        let result_dir = if command.subprocess {
            None
        } else {
            ResultDir::create()
                .ok()
                .filter(|dir| dir.file().to_str().is_some())
        };
        if let Some(dir) = &result_dir {
            leading.push(crate::dispatch::ENTRY_CHILD_ARG.to_string());
            leading.push(dir.file().to_string_lossy().into_owned());
        }
        leading.extend(command.sysargs.iter().cloned());
        // An in-process command (`replace == false`) runs inside the subaction's project
        // environment in Python, so the child gets that environment too.
        let (mut env, env_clear) = match command.env.as_ref().or(command.inherited_env.as_ref()) {
            Some(env) => (env_dict_to_strings(env), true),
            None => (Vec::new(), false),
        };
        if env_clear && meerschaum_core::self_exe::runs_in_python() {
            // `python -m meerschaum` must still find this package in the cleared environment.
            for key in ["PYTHONPATH", "PYTHONHOME"] {
                if let Some(value) = std::env::var_os(key)
                    && !env.iter().any(|(k, _)| k == key)
                {
                    env.push((key.to_string(), value.to_string_lossy().into_owned()));
                }
            }
        }
        if let Some(dir) = &result_dir {
            env.push((
                crate::dispatch::ENTRY_CHILD_ENV.to_string(),
                dir.file().to_string_lossy().into_owned(),
            ));
        }
        let spec = CommandSpec {
            program,
            args: leading,
            env,
            env_clear,
            quiet: command.capture_output,
            cwd: None,
        };
        let status = self.rt.borrow_mut().host.run(&spec);
        // The directory (and the file in it) is removed when `result_dir` drops.
        let written = result_dir
            .as_ref()
            .and_then(|dir| crate::dispatch::read_entry_child_result(&dir.file()));
        match (status, written) {
            (Ok(_), Some(result)) => result,
            (Ok(0), None) => SuccessTuple::success(),
            (Ok(code), None) => SuccessTuple::fail(format!("Command returned exit code {code}.")),
            (Err(e), _) => SuccessTuple::fail(e.to_string()),
        }
    }
}

/// Print compose's messages (warnings to stderr, info to stdout).
fn print_messages(rt: &mut Runtime<'_>, messages: &[Message]) {
    for m in messages {
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
}

/// A private directory for the result of one in-process command's child: created exclusively
/// (an existing path, e.g. one planted by another user, is never reused) with mode 0700 on Unix
/// under the per-user temporary directory, and removed with its contents on drop.
struct ResultDir {
    path: PathBuf,
}

impl ResultDir {
    fn create() -> std::io::Result<ResultDir> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let base = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or_default();
        let mut last_error = None;
        for _ in 0..16 {
            let path = base.join(format!(
                "mrsm-compose-{}-{}-{nanos:08x}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::SeqCst)
            ));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => return Ok(ResultDir { path }),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last_error = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last_error.unwrap_or_else(|| std::io::Error::other("no free name")))
    }

    /// The result file the child creates.
    fn file(&self) -> PathBuf {
        self.path.join("result.json")
    }
}

impl Drop for ResultDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
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
        let mut runner = MrsmRunner::new(&shared);
        let mut prompter = HostPrompter { rt: &shared };
        compose_native(&host, &compose_args, &mut runner, &mut prompter)
    };
    print_messages(rt, &output.messages.items);
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
