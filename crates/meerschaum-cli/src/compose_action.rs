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
use meerschaum_compose::lint::Severity;
use meerschaum_compose::ops::{JobInfo, JobOps, Prompter};
use meerschaum_compose::pipes::ProjectConfig;
use meerschaum_compose::project::{Host, Session};
use meerschaum_compose::subactions::{ComposeArgs, ComposeOutput, ExplainModel, Services, compose};
use meerschaum_core::SuccessTuple;
use meerschaum_core::value::Value;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::{CommandSpec, Runtime};
use crate::format::Formatter;
use crate::format::rich::console::Renderable;
use crate::format::rich::panel::Panel;
use crate::format::rich::table::{Column, MINIMAL, Table};
use crate::format::rich::text::Text;
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

/// The project's native plugin executors, opened the first time a `<type>:<label>` executor
/// is resolved.
struct LazyNativeExecutors {
    session: crate::native::Session,
    project: std::sync::Arc<ProjectConfig>,
    executors: std::sync::OnceLock<Option<crate::native_executors::NativeExecutors>>,
}

impl std::fmt::Debug for LazyNativeExecutors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyNativeExecutors")
            .field("executors", &self.executors.get())
            .finish()
    }
}

impl meerschaum_jobs::executor::CustomExecutors for LazyNativeExecutors {
    fn resolve(
        &self,
        executor_keys: &str,
    ) -> Option<Result<Box<dyn meerschaum_jobs::executor::JobExecutor>, String>> {
        self.executors
            .get_or_init(|| {
                let config = self.project.registry();
                let natives = self
                    .session
                    .native_plugins(config)
                    .ok()
                    .filter(|n| !n.executor_types().is_empty())?;
                Some(crate::native_executors::NativeExecutors::new(
                    natives, config,
                ))
            })
            .as_ref()?
            .resolve(executor_keys)
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
            session.clone(),
            crate::native::remote_jobs::CliRemoteResolver::section(config),
        );
        let mut context = meerschaum_jobs::JobsContext::new(
            meerschaum_jobs::config::JobPaths::from_config(config),
            meerschaum_jobs::config::JobsConfig::from_config(config),
            meerschaum_jobs::Launcher::from_env(),
        )
        .with_remote(std::sync::Arc::new(resolver));
        // `<type>:<label>` executors of native plugins, opened on first use: `compose explain`
        // resolves no executor and must not build the plugin manifest under the root.
        context = context.with_executors(std::sync::Arc::new(LazyNativeExecutors {
            session,
            project: std::sync::Arc::new(project),
            executors: std::sync::OnceLock::new(),
        }));
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
/// in-process `register pipes` commands, which run in this process (`MrsmRunner::run_in_process`).
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
            #[cfg_attr(not(unix), allow(unused_mut))]
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

/// `pipe_repr(pipe, as_rich_text=True)` inside the project: `instance=` is shown unless the pipe
/// is on the project's default instance (v4 runs `explain` under the project's configuration).
fn pipe_repr(
    f: &Formatter<'_>,
    pipe: &meerschaum_compose::subactions::ExplainPipe,
    default_instance: &str,
) -> Text {
    let ansi = f.settings.ansi;
    let style = |key: &str| -> String {
        if ansi {
            f.formatting_str(&["pipes", "__repr__", "ansi", "styles", key])
        } else {
            String::new()
        }
    };
    let pipe_style = style("Pipe");
    let markup = |s: &str| {
        let mut t = Text::plain_text(s);
        if !pipe_style.is_empty() {
            t.stylize(pipe_style.as_str(), 0, None);
        }
        t
    };
    let mut text = markup("Pipe(");
    text.append_text(&Text::new(
        &format!("'{}'", pipe.connector),
        style("connector").as_str(),
    ));
    text.append_text(&markup(", "));
    text.append_text(&Text::new(
        &format!("'{}'", pipe.metric),
        style("metric").as_str(),
    ));
    if let Some(location) = &pipe.location {
        text.append_text(&Text::new(", ", style("punctuation").as_str()));
        text.append_text(&Text::new(
            &format!("'{location}'"),
            style("location").as_str(),
        ));
    }
    if pipe.instance != default_instance {
        text.append_text(&Text::new(", instance=", style("punctuation").as_str()));
        text.append_text(&Text::new(
            &format!("'{}'", pipe.instance),
            style("instance").as_str(),
        ));
    }
    text.append_text(&markup(")"));
    text
}

/// One project's `explain` table as v4 prints it: `rich` table (`box.MINIMAL`, expanded) of the
/// defined pipes (`pipe_repr` colors, instance panels) and their parameters (`rich` JSON).
pub fn render_explain(f: &Formatter<'_>, model: &ExplainModel) -> String {
    let instance_style = f.formatting_str(&["pipes", "__repr__", "ansi", "styles", "instance"]);
    let mut table = Table::new();
    table.title = Some(Text::new(
        &format!("Pipes in '{}'", model.project_name),
        "bold",
    ));
    table.box_chars = Some(MINIMAL);
    table.show_header = true;
    table.expand = true;
    table.collapse_padding = true;
    let mut headers = vec!["Defined Pipes", "Compose Parameters"];
    // PARITY: the instance rows carry an empty remote cell, so the column shows whenever there
    // is a pipe (v4 explain.py:249).
    if !model.instances.is_empty() {
        headers.push("Remote Parameters");
    }
    for header in &headers {
        table.add_column(Column {
            header: Text::plain_text(header),
            ..Column::default()
        });
    }
    let json = |map: &meerschaum_core::value::Map| -> Box<dyn Renderable> {
        let mut text = crate::native::rich_json::json_text(&Value::Map(map.clone()));
        text.no_wrap = Some(true);
        text.overflow = None;
        Box::new(text)
    };
    let blank = || -> Option<Box<dyn Renderable>> { Some(Box::new(Text::plain_text(""))) };
    for instance in &model.instances {
        let mut panel = Panel::new(Box::new(Text::new(&instance.keys, instance_style.as_str())));
        panel.title = Some(Text::plain_text("Instance"));
        let mut row: Vec<Option<Box<dyn Renderable>>> = vec![Some(Box::new(panel))];
        row.push(blank());
        row.push(blank());
        row.truncate(headers.len());
        table.add_row(row);
        table.add_row(vec![blank(), blank()]);
        for (i, pipe) in instance.pipes.iter().enumerate() {
            let mut text = pipe_repr(f, pipe, &model.default_instance);
            text.append_text(&Text::plain_text(&format!("\n\n{}\n", pipe.status)));
            let mut row: Vec<Option<Box<dyn Renderable>>> =
                vec![Some(Box::new(text)), Some(json(&pipe.local))];
            row.push(match &pipe.remote {
                Some(remote) => Some(json(remote)),
                None => blank(),
            });
            row.truncate(headers.len());
            table.add_row(row);
            if i + 1 == instance.pipes.len() {
                table.end_last_section();
            } else {
                table.add_row(vec![blank(), blank()]);
            }
        }
    }
    f.console().print(&table)
}

/// What the compose command line says that argparse leaves out: the compose flags and action
/// words may come in any order (`compose --file F up`), and `-f F` names a file when it is
/// followed by a YAML path (`-f` alone still follows the logs).
#[derive(Debug, Default, PartialEq)]
struct ComposeWords {
    /// The action words after `compose` (the subaction first).
    action: Vec<String>,
    /// `--file`, `--compose-file` or `-f <path>.yaml`.
    file: Option<PathBuf>,
    /// `-f` alone (force / follow the logs).
    force: bool,
    /// `--project <name>`.
    project: Option<String>,
    /// `--standalone`.
    standalone: bool,
    /// `--no-stagger`.
    no_stagger: bool,
}

/// The compose flags that take no value (so the words around them are still action words).
const FLAGS_WITHOUT_VALUE: &[&str] = &[
    "--dry",
    "-v",
    "--volumes",
    "--drop",
    "--presync",
    "--no-jobs",
    "--isolated",
    "--force",
    "-y",
    "--yes",
    "--noask",
    "--nopretty",
    "--debug",
    "--standalone",
    "--no-stagger",
];

fn is_yaml_path(s: &str) -> bool {
    !s.starts_with('-') && (s.ends_with(".yaml") || s.ends_with(".yml"))
}

fn compose_words(sysargs: &[String]) -> ComposeWords {
    let mut words = ComposeWords::default();
    let Some(start) = sysargs.iter().position(|a| a == "compose") else {
        return words;
    };
    let mut collecting = true;
    let mut i = start + 1;
    while i < sysargs.len() {
        let arg = sysargs[i].as_str();
        let next = sysargs.get(i + 1).map(String::as_str);
        match arg {
            "--file" | "--compose-file" => {
                if let Some(v) = next {
                    words.file = Some(PathBuf::from(v));
                    i += 1;
                }
            }
            "--env-file" | "--env" => i += usize::from(next.is_some()),
            "--project" => {
                if let Some(v) = next {
                    words.project = Some(v.to_string());
                    i += 1;
                }
            }
            "-f" => match next {
                Some(v) if is_yaml_path(v) => {
                    words.file = Some(PathBuf::from(v));
                    i += 1;
                }
                _ => words.force = true,
            },
            "--standalone" => words.standalone = true,
            "--no-stagger" => words.no_stagger = true,
            _ if arg.starts_with("--project=") => {
                words.project = Some(arg["--project=".len()..].to_string());
            }
            _ if arg.starts_with("--file=") || arg.starts_with("--compose-file=") => {
                let value = arg.split_once('=').map(|(_, v)| v).unwrap_or_default();
                words.file = Some(PathBuf::from(value));
            }
            _ if FLAGS_WITHOUT_VALUE.contains(&arg) => {}
            _ if arg.starts_with('-') => collecting = false,
            _ => {
                if collecting {
                    words.action.push(arg.to_string());
                }
            }
        }
        i += 1;
    }
    words
}

fn flag(args: &ArgsMap, key: &str) -> bool {
    args.get(key).is_some_and(PyValue::is_truthy)
}

fn str_list(args: &ArgsMap, key: &str) -> Vec<String> {
    match args.get(key) {
        Some(PyValue::List(items)) => items.iter().map(PyValue::py_str).collect(),
        Some(PyValue::Str(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// `-l` values (`None` for a null location).
fn location_list(args: &ArgsMap, key: &str) -> Vec<Option<String>> {
    match args.get(key) {
        Some(PyValue::List(items)) => items
            .iter()
            .map(|v| match v {
                PyValue::None => None,
                other => {
                    let s = other.py_str();
                    (s != "None").then_some(s)
                }
            })
            .collect(),
        _ => Vec::new(),
    }
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
    let parsed_action: Vec<String> = args
        .get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    let sysargs: Vec<String> = args
        .get("sysargs")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    // The flags may come before the subaction (`compose --file F up`): argparse then leaves the
    // action words out, so they are read from the command line.
    let words = compose_words(&sysargs);
    let action = if words.action.len() > parsed_action.len() {
        words.action.clone()
    } else {
        parsed_action
    };
    let dry = flag(args, "dry");
    let settings = rt.settings();
    let file = path(args, "file").or_else(|| words.file.clone());
    // `-f <path>.yaml` names the file; only a bare `-f` is the force / follow flag.
    let force = if words.file.is_some() && path(args, "file").is_none() {
        words.force
    } else {
        flag(args, "force")
    };
    let compose_args = ComposeArgs {
        action: action.clone(),
        sysargs,
        file,
        env_file: path(args, "env_file"),
        dry,
        force,
        presync: flag(args, "presync"),
        no_jobs: flag(args, "no_jobs"),
        isolated: flag(args, "isolated"),
        drop: flag(args, "drop"),
        yes: flag(args, "yes"),
        noask: flag(args, "noask"),
        nopretty: flag(args, "nopretty"),
        debug: flag(args, "debug"),
        width: Some(settings.width),
        standalone: words.standalone || flag(args, "standalone"),
        project: words.project.clone(),
        no_stagger: words.no_stagger || flag(args, "no_stagger"),
        connector_keys: str_list(args, "connector_keys"),
        metric_keys: str_list(args, "metric_keys"),
        location_keys: location_list(args, "location_keys"),
        tags: str_list(args, "tags"),
        mrsm_instance: args
            .get("mrsm_instance")
            .filter(|v| v.is_truthy())
            .map(PyValue::py_str),
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
    if !output.explain_models.is_empty() {
        let text = rt.with_formatter(|f| {
            output
                .explain_models
                .iter()
                .map(|model| render_explain(f, model))
                .collect::<Vec<_>>()
                .join("\n")
        });
        rt.print(&text);
        if !text.ends_with('\n') {
            rt.print("\n");
        }
    } else if let Some(explain) = &output.explain {
        rt.print(explain);
        if !explain.ends_with('\n') {
            rt.print("\n");
        }
    }
    // Rust-only: the `explain` lint (v4 has none): warnings to stderr, notes (overrides, what
    // `up` would install) like `info()`.
    for finding in &output.lint {
        match finding.severity {
            Severity::Warning => {
                let text = rt.with_formatter(|f| f.warn(&finding.to_string()));
                rt.eprint(&text);
            }
            Severity::Info => {
                let text = rt.with_formatter(|f| f.info(&finding.to_string(), true));
                rt.print(&text);
            }
        }
    }
    output
        .result
        .map_err(|e| ActionError::new(e.kind.python_name(), e.message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> ComposeWords {
        let sysargs: Vec<String> = line.split_whitespace().map(String::from).collect();
        compose_words(&sysargs)
    }

    #[test]
    fn the_action_words_survive_flags_in_front_of_them() {
        assert_eq!(words("compose up").action, vec!["up"]);
        assert_eq!(words("compose --file a.yaml up").action, vec!["up"]);
        assert_eq!(
            words("compose --project x --file a.yaml show pipes").action,
            vec!["show", "pipes"]
        );
        assert_eq!(words("compose --dry -v up").action, vec!["up"]);
        // An unknown flag ends the action: its value is not an action word.
        assert_eq!(
            words("compose sync pipes -c sql:a").action,
            vec!["sync", "pipes"]
        );
        assert_eq!(
            words("compose -c sql:a sync pipes").action,
            Vec::<String>::new()
        );
    }

    #[test]
    fn f_names_a_file_only_before_a_yaml_path() {
        let w = words("compose up -f other.yaml");
        assert_eq!(w.file, Some(PathBuf::from("other.yaml")));
        assert!(!w.force);
        assert_eq!(w.action, vec!["up"]);
        let w = words("compose -f up");
        assert_eq!(w.file, None);
        assert!(w.force, "a bare -f still follows the logs");
        let w = words("compose up -f");
        assert!(w.force && w.file.is_none());
        assert_eq!(
            words("compose --file=x.yml up").file,
            Some(PathBuf::from("x.yml"))
        );
        assert_eq!(
            words("compose --compose-file x.yml up").file,
            Some(PathBuf::from("x.yml"))
        );
    }

    #[test]
    fn compose_flags_are_read_from_the_command_line() {
        let w = words("compose up --project weather --standalone --no-stagger");
        assert_eq!(w.project.as_deref(), Some("weather"));
        assert!(w.standalone && w.no_stagger);
        assert_eq!(
            words("compose up --project=a").project.as_deref(),
            Some("a")
        );
    }
}
