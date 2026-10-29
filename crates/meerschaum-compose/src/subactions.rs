//! The compose subactions: a port of `meerschaum/actions/compose.py`,
//! `meerschaum/compose/subactions/*.py` and `meerschaum/compose/utils/__init__.py::init`.
//!
//! [`compose`] is the entrypoint (`mrsm compose <subaction> ...`): it locates and resolves the
//! project, initializes its root directory, and runs the subaction. Commands are run through
//! the [`CommandRunner`]; registrations and jobs through [`InstanceOps`] and [`JobOps`].

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use meerschaum_core::pipe::Pipe;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps, py_str};
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::template::TemplateResolver;
use meerschaum_core::value::{Map, Value, apply_patch_maps};

use crate::command::{CommandRunner, MrsmCommand, RunOptions, build_command, execute};
use crate::error::{ComposeError, ErrorKind, Messages};
use crate::jobs::{get_jobs_commands, get_project_job_names, job_compose_root, job_project_name};
use crate::lint::{LintInput, LintWarning, lint};
use crate::ops::{InstanceOps, JobInfo, JobOps, PipeFilters, Prompter};
use crate::pipes::{
    ProjectConfig, build_pipes, custom_connectors, get_defined_pipes_meta, instance_pipes,
    location_arg,
};
use crate::plugins::{
    check_and_install_plugins, get_installed_plugins, warn_if_compose_plugin_installed,
};
use crate::project::{
    Host, LoadOptions, PipeKey, Project, Session, config_has_changed, infer_compose_file_path,
    json_sorted_compact, write_config_cache,
};
use crate::refs::{self, PipeSource};
use crate::render::{Renderable, Row, Table};
use crate::tree::{Tree, TreeOptions, load_tree, parent_chain, pipe_key};

/// The environment variable the startup admission gate reads to classify a start (D23, the
/// jobs crate's `startup::KIND_ENV`): `bulk` when compose starts several jobs or projects
/// together (the gate applies its jitter), `none` for `--no-stagger` (no gate at all).
pub const START_KIND_VAR: &str = "MRSM_JOB_START_KIND";

/// The subaction modules Python discovers (`get_subactions()`).
pub const SUBACTIONS: &[&str] = &[
    "default", "down", "explain", "init", "logs", "ps", "run", "up",
];

/// The job actions scoped to the project's jobs (`_SCOPED_JOB_ACTIONS`).
pub const SCOPED_JOB_ACTIONS: &[&str] = &["start", "stop", "pause", "restart", "delete"];

/// `complete_compose(action)`: completions for the subaction name.
pub fn complete_compose(action: &[String]) -> Vec<String> {
    let mut subs: Vec<String> = SUBACTIONS
        .iter()
        .filter(|s| **s != "default")
        .map(|s| s.to_string())
        .collect();
    subs.sort();
    match action.first() {
        None => subs,
        Some(first) => subs
            .into_iter()
            .filter(|s| s.starts_with(first.as_str()) && s != first)
            .collect(),
    }
}

/// The compose flags and arguments (`mrsm compose ...`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ComposeArgs {
    /// The action words after `compose` (the subaction first).
    pub action: Vec<String>,
    /// The full command line (`['compose', ...]`), used by the passthrough subaction and to
    /// forward flags to the initial syncs.
    pub sysargs: Vec<String>,
    /// `--file` / `--compose-file`.
    pub file: Option<PathBuf>,
    /// `--env-file` / `--env`.
    pub env_file: Option<PathBuf>,
    /// `--dry`.
    pub dry: bool,
    /// `-f` / `--force` (`up -f` follows the logs).
    pub force: bool,
    /// `--presync`.
    pub presync: bool,
    /// `--no-jobs`.
    pub no_jobs: bool,
    /// `--isolated`.
    pub isolated: bool,
    /// `-v` / `--volumes` (`down -v` drops the pipes).
    pub drop: bool,
    /// `-y` / `--yes`.
    pub yes: bool,
    /// `--noask`.
    pub noask: bool,
    /// `--nopretty`.
    pub nopretty: bool,
    /// `--debug`.
    pub debug: bool,
    /// `--allow-protected` (forwarded to the commands and jobs compose runs).
    pub allow_protected: bool,
    /// The console width for `explain`.
    pub width: Option<usize>,
    /// `--standalone`: ignore the file's `parent:` and run it on its own.
    pub standalone: bool,
    /// `--project <name>`: limit the operation to one project of the tree.
    pub project: Option<String>,
    /// `--no-stagger`: start jobs without the startup jitter and slots.
    pub no_stagger: bool,
    /// `init -c`.
    pub connector_keys: Vec<String>,
    /// `init -m`.
    pub metric_keys: Vec<String>,
    /// `init -l`.
    pub location_keys: Vec<Option<String>>,
    /// `init -t`.
    pub tags: Vec<String>,
    /// `init -i`.
    pub mrsm_instance: Option<String>,
}

/// The live services a subaction uses.
pub struct Services<'a> {
    /// Runs Meerschaum commands.
    pub runner: &'a mut dyn CommandRunner,
    /// Pipe registrations.
    pub instance: &'a mut dyn InstanceOps,
    /// Background jobs.
    pub jobs: &'a mut dyn JobOps,
    /// Interactive questions.
    pub prompter: &'a mut dyn Prompter,
}

/// Everything a subaction produced.
#[derive(Debug, Clone)]
pub struct ComposeOutput {
    /// The result, or the exception Python would raise.
    pub result: Result<SuccessTuple, ComposeError>,
    /// The `warn()` / `info()` messages, in order.
    pub messages: Messages,
    /// The commands run (or, under `--dry`, that would run), in order.
    pub commands: Vec<MrsmCommand>,
    /// The resolved project, if it could be loaded.
    pub project: Option<Project>,
    /// The pipe metadata (`get_defined_pipes(as_meta=True)`), if computed.
    pub pipes_meta: Option<Vec<Map>>,
    /// The defined pipes, if constructed.
    pub pipes: Option<Vec<Pipe>>,
    /// The `explain` table text.
    pub explain: Option<String>,
    /// The lint findings (`explain` only).
    pub lint: Vec<LintWarning>,
    /// Files written (`init` creates the compose file).
    pub written_files: Vec<(PathBuf, String)>,
    /// The projects that ran, in the order they ran, with their overrides applied.
    pub projects: Vec<Project>,
    /// The `explain` tables as data (one per project), for a renderer with colors.
    pub explain_models: Vec<ExplainModel>,
}

/// One project's `explain` table as data.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainModel {
    /// The project name (the table title is `Pipes in '<name>'`).
    pub project_name: String,
    /// The console width.
    pub width: usize,
    /// The project's default instance (a pipe on it is shown without `instance=`).
    pub default_instance: String,
    /// The pipes grouped by instance.
    pub instances: Vec<ExplainInstance>,
}

/// The defined pipes of one instance.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainInstance {
    /// The instance keys.
    pub keys: String,
    /// The pipes.
    pub pipes: Vec<ExplainPipe>,
}

/// One defined pipe in `explain`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainPipe {
    /// Connector keys.
    pub connector: String,
    /// Metric key.
    pub metric: String,
    /// Location key.
    pub location: Option<String>,
    /// Instance keys.
    pub instance: String,
    /// The registration status (`✅ Up-to-date`, ...).
    pub status: String,
    /// The parameters in the compose file.
    pub local: Map,
    /// The registered parameters, when they differ.
    pub remote: Option<Map>,
}

impl ComposeOutput {
    fn new() -> ComposeOutput {
        ComposeOutput {
            result: Ok(SuccessTuple::success()),
            messages: Messages::default(),
            commands: Vec::new(),
            project: None,
            pipes_meta: None,
            pipes: None,
            explain: None,
            lint: Vec::new(),
            written_files: Vec::new(),
            projects: Vec::new(),
            explain_models: Vec::new(),
        }
    }
}

/// A runner that records the commands while delegating to the real runner.
struct Tee<'a> {
    inner: &'a mut dyn CommandRunner,
    commands: Vec<MrsmCommand>,
}

impl CommandRunner for Tee<'_> {
    fn run(&mut self, command: &MrsmCommand) -> SuccessTuple {
        self.commands.push(command.clone());
        self.inner.run(command)
    }

    fn show_messages(&mut self, messages: &[crate::error::Message]) -> bool {
        self.inner.show_messages(messages)
    }
}

struct Ctx<'a, 'b> {
    host: &'a Host,
    session: &'a mut Session,
    args: &'a ComposeArgs,
    runner: Tee<'b>,
    instance: &'b mut dyn InstanceOps,
    jobs: &'b mut dyn JobOps,
    prompter: &'b mut dyn Prompter,
    out: ComposeOutput,
    /// The project being run is the primary one (the root, or the only one selected): its
    /// pipes are what [`ComposeOutput::pipes`] reports.
    primary: bool,
    /// The run spans several projects.
    multi: bool,
    /// Root directories already initialized in this run.
    initialized_roots: Vec<PathBuf>,
    /// Start jobs as a bulk start (the startup gate applies its jitter).
    bulk_start: bool,
    /// Where each pipe of the project being run is defined, aligned with its pipes.
    pipe_sources: Vec<PipeSource>,
}

impl Ctx<'_, '_> {
    fn run(
        &mut self,
        args: &[String],
        project: &Project,
        opts: RunOptions,
    ) -> Result<SuccessTuple, ComposeError> {
        let opts = RunOptions {
            debug: self.args.debug,
            allow_protected: self.args.allow_protected,
            ..opts
        };
        if !self.out.messages.items.is_empty()
            && self.runner.show_messages(&self.out.messages.items)
        {
            self.out.messages.items.clear();
        }
        let mut command = build_command(args, project, opts, &mut self.out.messages)?;
        let mut extra: Vec<(&str, &str)> = Vec::new();
        if self.args.no_stagger {
            extra.push((START_KIND_VAR, "none"));
        } else if self.bulk_start {
            extra.push((START_KIND_VAR, "bulk"));
        }
        for (key, value) in extra {
            for env in [command.env.as_mut(), command.inherited_env.as_mut()]
                .into_iter()
                .flatten()
            {
                env.insert(key.to_string(), Value::Str(value.to_string()));
            }
        }
        Ok(execute(&command, &mut self.runner))
    }

    /// `get_jobs()` on the project's executor; a failed read is the failed result to return.
    fn project_jobs(&mut self) -> Result<IndexMap<String, JobInfo>, SuccessTuple> {
        let jobs = self.jobs.try_get_jobs();
        for warning in self.jobs.take_warnings() {
            self.out.messages.warn(warning);
        }
        jobs.map_err(|e| {
            SuccessTuple::fail(format!(
                "Could not read the jobs of this project's executor: {e}"
            ))
        })
    }

    fn yes_no(
        &mut self,
        question: &str,
        default_yes: bool,
        noask: bool,
    ) -> Result<bool, ComposeError> {
        if self.args.yes {
            return Ok(true);
        }
        let env_noask = self
            .host
            .env
            .get("MRSM_NOASK")
            .is_some_and(|v| matches!(v.to_lowercase().as_str(), "1" | "true"));
        if noask || env_noask {
            return Ok(default_yes);
        }
        self.prompter
            .ask(question, default_yes)
            .ok_or_else(|| ComposeError::new(ErrorKind::Os, "EOF when reading a line"))
    }

    /// Jobs of `names` that were started from another root compose file: they are managed there.
    /// Returns the names to leave alone (each warned about once), unless `--force`.
    fn managed_elsewhere(
        &mut self,
        project: &Project,
        jobs: &IndexMap<String, JobInfo>,
        names: &[String],
    ) -> Vec<String> {
        if self.args.force {
            return Vec::new();
        }
        let ours = project.compose_root.as_deref().map(crate::project::posix);
        let mut skipped = Vec::new();
        for name in names {
            let Some(root) = jobs.get(name).and_then(job_compose_root) else {
                continue;
            };
            if ours.as_deref() == Some(root.as_str()) {
                continue;
            }
            self.out.messages.warn(format!(
                "The job '{name}' is managed by the root compose at {root}; rerun from there or pass `--force`."
            ));
            skipped.push(name.clone());
        }
        skipped
    }
}

/// `mrsm compose <action> ...`: run a subaction in the project.
///
/// `session` holds the per-process state (the config-cache memo and the one-time plugin
/// warning); reuse it across calls in one process.
pub fn compose(
    host: &Host,
    session: &mut Session,
    args: &ComposeArgs,
    services: Services,
) -> ComposeOutput {
    let Services {
        runner,
        instance,
        jobs,
        prompter,
    } = services;
    let mut ctx = Ctx {
        host,
        session,
        args,
        runner: Tee {
            inner: runner,
            commands: Vec::new(),
        },
        instance,
        jobs,
        prompter,
        out: ComposeOutput::new(),
        primary: true,
        multi: false,
        initialized_roots: Vec::new(),
        bulk_start: false,
        pipe_sources: Vec::new(),
    };
    warn_if_compose_plugin_installed(host, ctx.session, &mut ctx.out.messages);
    let subaction = args
        .action
        .first()
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    let result = do_subaction(&mut ctx, &subaction);
    let mut out = ctx.out;
    out.commands = ctx.runner.commands;
    out.result = result;
    out
}

/// The projects a run covers.
struct RunTree {
    tree: Tree,
    /// The pipes any project of the tree defines (for the `{{ Pipe() }}` lint).
    tree_pipes: Vec<PipeKey>,
    /// The projects to run, in the order they run.
    selected: Vec<usize>,
    /// The project whose pipes the output reports.
    primary: usize,
}

/// Locate the compose file, follow `parent:` to the topmost file, load the tree and pick the
/// projects the run covers.
fn load_run_tree(
    ctx: &mut Ctx,
    file: Option<&Path>,
    reverse: bool,
    strict: bool,
) -> Result<RunTree, ComposeError> {
    let args = ctx.args;
    let file = file.or(args.file.as_deref());
    let Some(compose_file) = infer_compose_file_path(file, &ctx.host.cwd, &mut ctx.out.messages)
    else {
        return Err(ComposeError::new(
            ErrorKind::FileNotFound,
            "No compose file could be found.\n    \
             Create a file mrsm-compose.yaml or specify a path with `--file`.",
        ));
    };
    let chain = if args.standalone {
        vec![compose_file.clone()]
    } else {
        parent_chain(&compose_file, &mut ctx.out.messages)
    };
    let redirected = chain.len() > 1;
    let attach: Vec<(PathBuf, PathBuf)> = chain
        .windows(2)
        .map(|w| (w[0].clone(), w[1].clone()))
        .collect();
    let top = chain[0].clone();
    let tree = load_tree(
        ctx.host,
        &TreeOptions {
            load: LoadOptions {
                file: Some(&top),
                env_file: if redirected {
                    None
                } else {
                    args.env_file.as_deref()
                },
                isolated: args.isolated,
                ..LoadOptions::default()
            },
            attach,
        },
        &mut ctx.out.messages,
    )?;
    // Everything but `explain` touches jobs (or reports them): a tree it cannot tell apart or
    // cannot fully read must not run, or one project's jobs are treated as another's stale ones
    // and an included project is silently left running by `down`.
    if strict && let Some(message) = tree.blocking_problems() {
        return Err(ComposeError::value(message));
    }

    let mut selected: Vec<usize> = if redirected {
        // The file has a parent: run from the parent's tree, touching only this project.
        tree.by_file(chain.last().expect("a chain has a file"))
            .into_iter()
            .collect()
    } else {
        tree.order()
    };
    if let Some(name) = &args.project {
        match tree.by_name(name) {
            Some(ix) => selected = vec![ix],
            None => {
                let names: Vec<String> = tree
                    .nodes
                    .iter()
                    .map(|n| format!("'{}'", n.project.project_name()))
                    .collect();
                return Err(ComposeError::value(format!(
                    "No project named '{name}' in this compose project (projects: {}).",
                    names.join(", ")
                )));
            }
        }
    }
    if selected.is_empty() {
        return Err(ComposeError::value(
            "The compose file could not be found in its parent's project.",
        ));
    }
    let primary = if selected.contains(&0) {
        0
    } else {
        *selected.last().expect("non-empty")
    };
    if reverse {
        selected.reverse();
    }
    ctx.out.projects = selected
        .iter()
        .map(|ix| tree.nodes[*ix].project.clone())
        .collect();
    ctx.out.project = Some(tree.nodes[primary].project.clone());
    let tree_pipes = if tree.is_multi() {
        tree.defined_pipe_keys(ctx.host)
    } else {
        Vec::new()
    };
    Ok(RunTree {
        tree,
        tree_pipes,
        selected,
        primary,
    })
}

fn do_subaction(ctx: &mut Ctx, subaction: &str) -> Result<SuccessTuple, ComposeError> {
    let selected = if SUBACTIONS.contains(&subaction) {
        subaction
    } else {
        "default"
    };
    if subaction == "init" {
        return compose_init(ctx);
    }
    let run = load_run_tree(ctx, None, selected == "down", selected != "explain")?;
    ctx.multi = run.selected.len() > 1;
    if selected == "explain" {
        // The overrides of the projects this run covers (and the tree-wide problems).
        let names: Vec<String> = run
            .selected
            .iter()
            .map(|ix| run.tree.nodes[*ix].project.project_name())
            .collect();
        ctx.out.lint.extend(
            run.tree
                .findings
                .iter()
                .filter(|f| {
                    names
                        .iter()
                        .any(|n| f.location == *n || f.location.starts_with(&format!("{n}:")))
                })
                .cloned(),
        );
    }
    // A shell (a bare `compose`) opens in one project: the root, or the only one selected.
    let ixs: Vec<usize> = if selected == "default" && ctx.args.action.is_empty() {
        vec![run.primary]
    } else {
        run.selected.clone()
    };
    let mut results: Vec<(String, SuccessTuple)> = Vec::new();
    for ix in &ixs {
        let project = run.tree.nodes[*ix].project.clone();
        ctx.primary = *ix == run.primary;
        if ctx.multi {
            ctx.out.messages.info(format!(
                "Project '{}' ({})...",
                project.project_name(),
                project.file.display()
            ));
        }
        let result = run_project(ctx, &run, *ix, &project, selected)?;
        let keep_going = selected == "down" || selected == "explain";
        let ok = result.success;
        results.push((project.project_name(), result));
        if !ok && !keep_going {
            break;
        }
    }
    Ok(combine_results(results, ctx.multi))
}

/// One result for a run over several projects: success only if every project succeeded.
fn combine_results(results: Vec<(String, SuccessTuple)>, multi: bool) -> SuccessTuple {
    if !multi {
        return results
            .into_iter()
            .next()
            .map(|(_, r)| r)
            .unwrap_or_else(SuccessTuple::success);
    }
    let success = results.iter().all(|(_, r)| r.success);
    let message = results
        .iter()
        .map(|(name, r)| format!("{name}: {}", r.message))
        .collect::<Vec<_>>()
        .join("\n");
    SuccessTuple { success, message }
}

fn run_project(
    ctx: &mut Ctx,
    run: &RunTree,
    ix: usize,
    project: &Project,
    selected: &str,
) -> Result<SuccessTuple, ComposeError> {
    let pc = ProjectConfig::new(ctx.host, project)?;
    // A host connector the file references and this host does not define fails before the root
    // is touched.
    if matches!(selected, "up" | "run")
        && let Some(message) = refs::blocking_message(&refs::findings(project, &pc, &[], &[]))
    {
        return Ok(SuccessTuple::fail(message));
    }
    // `explain` is side-effect free: no root initialization, no config cache.
    if selected != "explain" {
        init_root(ctx, project)?;
    }
    ctx.instance
        .try_configure(ctx.host, project)
        .map_err(ComposeError::value)?;
    ctx.jobs
        .configure(ctx.host, project)
        .map_err(ComposeError::value)?;
    match selected {
        "up" => compose_up(ctx, project, &pc, UpMode::from_args(ctx.args)),
        "run" => compose_up(
            ctx,
            project,
            &pc,
            UpMode {
                no_jobs: true,
                presync: true,
                ..UpMode::from_args(ctx.args)
            },
        ),
        "down" => compose_down(ctx, project, &pc),
        "explain" => compose_explain(ctx, project, &pc, &run.tree_pipes),
        "ps" | "logs" => {
            let verb = if selected == "ps" { "jobs" } else { "logs" };
            let mut a = vec!["show".to_string(), verb.to_string()];
            if ctx.multi {
                // Projects share one root: name the project's own jobs.
                let jobs = match ctx.project_jobs() {
                    Ok(jobs) => jobs,
                    Err(failed) => return Ok(failed),
                };
                let names =
                    get_project_job_names(&project.config, &[], &jobs, &mut ctx.out.messages)?;
                if names.is_empty() {
                    return Ok(SuccessTuple::ok(format!(
                        "No jobs in compose project '{}'.",
                        project.project_name()
                    )));
                }
                a.extend(names);
            }
            if ctx.args.nopretty {
                a.push("--nopretty".into());
            }
            // `logs` replaces the environment; `ps` does not (as in v4).
            ctx.run(
                &a,
                project,
                RunOptions {
                    replace: selected != "ps",
                    ..RunOptions::default()
                },
            )
        }
        _ => compose_default(ctx, project, run, ix),
    }
}

/// `init_root(compose_config)`: create the root directory, run `show version` once, install
/// plugin requirements in a fresh root, and refresh the config cache.
fn init_root(ctx: &mut Ctx, project: &Project) -> Result<bool, ComposeError> {
    let root = project.root_dir();
    let fresh = !root.exists();
    if fresh {
        // PARITY: `mkdir(exist_ok=True)` without `parents=True`.
        std::fs::create_dir(&root).map_err(|e| {
            ComposeError::new(
                ErrorKind::FileNotFound,
                format!("[Errno 2] {e}: '{}'", root.display()),
            )
        })?;
        ctx.out.messages.info(format!(
            "Initializing Meerschaum root directory:\n    {}\n    This should only take a few seconds...",
            root.display()
        ));
    }
    // Projects of a tree share a root: initialize it once per run.
    let already = ctx.initialized_roots.contains(&root);
    let mut success = true;
    if !already {
        ctx.initialized_roots.push(root.clone());
        let result = ctx.run(
            &["show".into(), "version".into(), "--no-daemon".into()],
            project,
            RunOptions {
                capture_output: true,
                ..RunOptions::default()
            },
        )?;
        success = result.success;
        if fresh && !get_installed_plugins(project)?.is_empty() {
            ctx.out
                .messages
                .info("Installing required packages for plugins...");
            ctx.run(
                &["install".into(), "required".into()],
                project,
                RunOptions::default(),
            )?;
        }
    }
    if config_has_changed(project, ctx.session) {
        let _ = write_config_cache(project);
    }
    Ok(success)
}

fn defined_pipes(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
) -> Result<Vec<Pipe>, ComposeError> {
    let metas = get_defined_pipes_meta(&project.config, pc, true, &mut ctx.out.messages)?;
    if ctx.primary && ctx.out.pipes_meta.is_none() {
        ctx.out.pipes_meta = Some(metas.clone());
    }
    let all = build_pipes(&metas, &pc.pipe_defaults())?;
    let entries = crate::lint::pipe_entries(&project.document);
    let mut kept_metas = Vec::new();
    let mut pipes = Vec::new();
    let mut sources = Vec::new();
    for (i, (meta, pipe)) in metas.into_iter().zip(all).enumerate() {
        // An including project's definition of the same pipe replaces this project's.
        if project.excluded_pipes.contains(&pipe_key(&pipe)) {
            continue;
        }
        let label = entries
            .get(i)
            .map(|(section, ix, _)| format!("{section}[{ix}]"))
            .unwrap_or_else(|| format!("pipes[{i}]"));
        sources.push(PipeSource {
            label,
            meta: meta.clone(),
        });
        kept_metas.push(meta);
        pipes.push(pipe);
    }
    ctx.pipe_sources = sources;
    if ctx.primary && !project.excluded_pipes.is_empty() {
        ctx.out.pipes_meta = Some(kept_metas);
    }
    if ctx.primary && ctx.out.pipes.is_none() {
        ctx.out.pipes = Some(pipes.clone());
    }
    Ok(pipes)
}

fn build_custom_connectors(ctx: &mut Ctx, project: &Project) -> Result<(), ComposeError> {
    for (keys, attrs) in custom_connectors(&project.config)? {
        ctx.instance
            .build_connector(&keys, &attrs)
            .map_err(ComposeError::value)?;
    }
    Ok(())
}

/// The unresolved references of the project being run (see [`refs::findings`]).
fn project_findings(ctx: &Ctx, project: &Project, pc: &ProjectConfig) -> Vec<LintWarning> {
    let installed = get_installed_plugins(project).unwrap_or_default();
    refs::findings(project, pc, &ctx.pipe_sources, &installed)
}

/// `mrsm.get_pipes(tags=tags, instance=...)`, raising when the instance cannot be reached.
fn get_pipes_or_raise(
    ctx: &mut Ctx,
    instance_keys: &str,
    tags: &[String],
) -> Result<Vec<Pipe>, ComposeError> {
    ctx.instance
        .try_get_pipes(instance_keys, tags)
        .map_err(|e| ComposeError::new(ErrorKind::Exception, e))
}

fn tags_list(params: &Map) -> Vec<Value> {
    match params.get("tags") {
        Some(Value::List(l)) => l.clone(),
        _ => Vec::new(),
    }
}

/// Python's `==` for JSON-like values (`1 == 1.0`, `True == 1`, dicts ignore order).
pub fn py_eq(a: &Value, b: &Value) -> bool {
    fn num(v: &Value) -> Option<f64> {
        match v {
            Value::Bool(b) => Some(f64::from(u8::from(*b))),
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
    match (a, b) {
        (Value::Map(x), Value::Map(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| py_eq(v, w)))
        }
        (Value::List(x), Value::List(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| py_eq(v, w))
        }
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => match (num(a), num(b)) {
            (Some(x), Some(y)) => x == y,
            _ => false,
        },
    }
}

/// A resolver over the instance for `pipe.parameters` (`{{ Pipe() }}` references load the
/// referenced pipes' registered parameters; table references need a live connector).
struct InstanceResolver<'a> {
    instance: std::cell::RefCell<&'a mut dyn InstanceOps>,
    defaults: meerschaum_core::pipe::PipeDefaults,
    config: &'a meerschaum_core::Config,
}

impl TemplateResolver for InstanceResolver<'_> {
    fn defaults(&self) -> meerschaum_core::pipe::PipeDefaults {
        self.defaults.clone()
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.instance.borrow_mut().table_reference(pipe)
    }

    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        let mut out = pipe.clone();
        if let Some(params) = self.instance.borrow_mut().get_parameters(pipe) {
            out.set_parameters(apply_patch_maps(&params, pipe.raw_parameters()));
        }
        out
    }

    fn config(&self) -> Option<&meerschaum_core::Config> {
        Some(self.config)
    }
}

/// `pipe.parameters` (the property): the registered parameters patched with the local ones,
/// with references, `{{ Pipe() }}` templates and `MRSM{}` symlinks resolved.
fn effective_parameters(ctx: &mut Ctx, pc: &ProjectConfig, pipe: &Pipe) -> Map {
    let local = pipe.raw_parameters().clone();
    let remote = if pipe.temporary {
        None
    } else {
        ctx.instance.get_parameters(pipe)
    };
    let merged = match remote {
        Some(r) => apply_patch_maps(&r, &local),
        None => local,
    };
    let mut with = pipe.clone();
    with.set_parameters(merged.clone());
    let resolver = InstanceResolver {
        instance: std::cell::RefCell::new(&mut *ctx.instance),
        defaults: pc.pipe_defaults(),
        config: pc.registry(),
    };
    match with.resolved_parameters(&resolver, Some(pc.registry())) {
        Ok(resolved) => {
            for w in resolved.warnings {
                ctx.out.messages.warn(w);
            }
            resolved.parameters
        }
        Err(_) => merged,
    }
}

/// Flags for `compose up`.
#[derive(Debug, Clone, Copy)]
struct UpMode {
    dry: bool,
    force: bool,
    presync: bool,
    no_jobs: bool,
}

impl UpMode {
    fn from_args(args: &ComposeArgs) -> UpMode {
        UpMode {
            dry: args.dry,
            force: args.force,
            presync: args.presync,
            no_jobs: args.no_jobs,
        }
    }
}

fn plural(n: usize) -> &'static str {
    if n != 1 { "s" } else { "" }
}

/// `compose up` (and `compose run`, which is `up --no-jobs --presync`).
fn compose_up(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
    mode: UpMode,
) -> Result<SuccessTuple, ComposeError> {
    let no_daemon_flags: Vec<String> =
        if project.config.get("isolation").and_then(Value::as_str) == Some("subprocess") {
            vec!["--no-daemon".into()]
        } else {
            Vec::new()
        };

    // A host connector the compose file references and this host does not define fails before
    // anything is installed.
    if let Some(message) = refs::blocking_message(&refs::findings(project, pc, &[], &[])) {
        return Ok(SuccessTuple::fail(message));
    }

    let result = check_and_install_plugins(
        project,
        pc,
        None,
        ctx.args.debug,
        &mut ctx.runner,
        &mut ctx.out.messages,
    )?;
    if !result.success {
        return Ok(result);
    }

    build_custom_connectors(ctx, project)?;
    let pipes = defined_pipes(ctx, project, pc)?;
    // Refuse to start on a connector or plugin that is not there, naming it and where it is
    // referenced (plugins listed under `plugins:` were installed above).
    let findings = project_findings(ctx, project, pc);
    if let Some(message) = refs::blocking_message(&findings) {
        return Ok(SuccessTuple::fail(message));
    }
    // Every job `up` would start is checked before anything changes: a job of another project
    // with one of its names refuses the whole `up` (no pipe is registered or synced first).
    if !mode.dry && !mode.no_jobs {
        // The messages of this look are shown only with a refusal: the jobs are listed again
        // (and their warnings shown) when they start.
        let mark = ctx.out.messages.items.len();
        let jobs_commands = get_jobs_commands(&project.config, &pipes, &mut Messages::default())?;
        let existing = match ctx.project_jobs() {
            Ok(jobs) => jobs,
            Err(failed) => return Ok(failed),
        };
        let owned =
            get_project_job_names(&project.config, &pipes, &existing, &mut Messages::default())?;
        if let Some(refusal) = foreign_collisions(&jobs_commands, &existing, &owned) {
            return Ok(refusal);
        }
        ctx.out.messages.items.truncate(mark);
    }
    let groups = instance_pipes(&pipes);
    let project_name = project.project_name();
    let explicit_jobs = project.config.get("jobs").is_some_and(Value::truthy);
    let tags = vec![project_name.clone()];

    let mut remote_instance_pipes: IndexMap<String, Vec<Pipe>> = IndexMap::new();
    for inst in groups.keys() {
        let remote = get_pipes_or_raise(ctx, inst, &tags)?;
        remote_instance_pipes.insert(inst.clone(), remote);
    }

    let mut updated: Vec<Pipe> = Vec::new();
    for pipe in &pipes {
        let mut updated_registration = false;
        let registered = remote_instance_pipes
            .get(pipe.instance_keys())
            .is_some_and(|rs| {
                rs.iter().any(|r| {
                    r.connector_keys() == pipe.connector_keys()
                        && r.metric_key() == pipe.metric_key()
                        && r.location_key() == pipe.location_key()
                })
            });
        let remote_params = ctx.instance.get_parameters(pipe).unwrap_or_default();
        let local_params = pipe.raw_parameters().clone();
        let local_str = json_sorted_compact(&Value::Map(local_params.clone()));
        let remote_str = json_sorted_compact(&Value::Map(remote_params.clone()));

        if pipe.temporary {
            ctx.out.messages.info(format!(
                "{pipe} is temporary, will not modify registration."
            ));
        } else if !registered {
            let existing_id = ctx.instance.get_id(pipe);
            if existing_id.is_some() {
                ctx.out.messages.info(format!(
                    "{pipe} already exists on '{}'; adding tag '{project_name}'...",
                    pipe.instance_keys()
                ));
                let fresh = ctx
                    .instance
                    .get_parameters(pipe)
                    .unwrap_or_else(|| remote_params.clone());
                let effective = effective_parameters(ctx, pc, pipe);
                let mut merged_tags: Vec<Value> = Vec::new();
                for t in tags_list(&fresh).into_iter().chain(tags_list(&effective)) {
                    if !merged_tags.contains(&t) {
                        merged_tags.push(t);
                    }
                }
                let mut merged = effective.clone();
                merged.insert("tags".into(), Value::List(merged_tags));
                let mut remote_pipe = pipe.clone();
                remote_pipe.set_parameters(merged);
                let edit = ctx.instance.edit_pipe(&remote_pipe);
                if !edit.success {
                    ctx.out.messages.warn(format!(
                        "Failed to add tag '{project_name}' to {pipe}:\n{}",
                        edit.message
                    ));
                }
            } else {
                ctx.out.messages.info(format!("Registering {pipe}..."));
                let params = effective_parameters(ctx, pc, pipe);
                let mut a: Vec<String> = vec![
                    "register".into(),
                    "pipes".into(),
                    "-c".into(),
                    pipe.connector_keys().into(),
                    "-m".into(),
                    pipe.metric_key().into(),
                    "-l".into(),
                    location_arg(pipe),
                    "-i".into(),
                    pipe.instance_keys().into(),
                    "--params".into(),
                    json_dumps(&Value::Map(params), JsonStyle::Compact, true),
                    "--noask".into(),
                ];
                a.extend(no_daemon_flags.iter().cloned());
                let r = ctx.run(
                    &a,
                    project,
                    RunOptions {
                        replace: false,
                        subprocess: Some(false),
                        ..RunOptions::default()
                    },
                )?;
                if !r.success {
                    ctx.out.messages.warn(format!("Failed to register {pipe}."));
                }
            }
            updated_registration = true;
        } else if local_str != remote_str {
            ctx.out
                .messages
                .info(format!("Updating parameters for {pipe}..."));
            let edit = ctx.instance.edit_pipe(pipe);
            if !edit.success {
                ctx.out.messages.warn(format!("Failed to edit {pipe}."));
            }
            updated_registration = true;
        }

        if updated_registration || mode.presync || pipe.temporary {
            updated.push(pipe.clone());
        }
    }

    // Untag the pipes which are tagged but no longer defined.
    for inst in groups.keys() {
        let tagged = get_pipes_or_raise(ctx, inst, &tags)?;
        for mut tagged_pipe in tagged {
            if pipes.contains(&tagged_pipe) {
                continue;
            }
            let params = ctx
                .instance
                .get_parameters(&tagged_pipe)
                .map(|r| apply_patch_maps(&r, tagged_pipe.raw_parameters()))
                .unwrap_or_else(|| tagged_pipe.raw_parameters().clone());
            let Some(Value::List(current)) = params.get("tags").cloned() else {
                ctx.out.messages.warn(format!(
                    "{tagged_pipe} was incorrectly tagged with '{project_name}'..."
                ));
                continue;
            };
            let remaining: Vec<Value> = current
                .into_iter()
                .filter(|t| py_str(t) != project_name)
                .collect();
            let mut new_params = params.clone();
            new_params.insert("tags".into(), Value::List(remaining));
            tagged_pipe.set_parameters(new_params);
            ctx.out.messages.info(format!(
                "Removing tag '{project_name}' from {tagged_pipe}..."
            ));
            ctx.instance.edit_pipe(&tagged_pipe);
        }
    }

    if mode.dry {
        return Ok(SuccessTuple::ok(format!(
            "Updated parameters for {} pipe{}{}{} instance{}.",
            pipes.len(),
            plural(pipes.len()),
            if groups.len() != 1 {
                " across "
            } else {
                " on "
            },
            groups.len(),
            plural(groups.len()),
        )));
    }

    let mut ran_verification_sync = false;
    if mode.presync
        || (!explicit_jobs && !updated.is_empty() && config_has_changed(project, ctx.session))
    {
        ran_verification_sync = true;
        let r = run_initial_syncs(ctx, project, &updated)?;
        if !r.success {
            return Ok(r);
        }
    }

    if mode.no_jobs {
        let msg = if ran_verification_sync {
            format!(
                "Synced {} pipe{} across {} instance{}.",
                updated.len(),
                plural(updated.len()),
                groups.len(),
                plural(groups.len())
            )
        } else if !updated.is_empty() {
            // PARITY: Python never pluralizes "instance" here (up.py:265).
            format!(
                "Updated {} pipe{} across {} instance.",
                updated.len(),
                plural(updated.len()),
                groups.len()
            )
        } else {
            "Nothing to do.".to_string()
        };
        return Ok(SuccessTuple::ok(msg));
    }

    let mut jobs_commands = get_jobs_commands(&project.config, &pipes, &mut ctx.out.messages)?;
    let existing = match ctx.project_jobs() {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let to_delete =
        get_project_job_names(&project.config, &pipes, &existing, &mut ctx.out.messages)?;
    // Checked again: another project may have started a job while the pipes synced.
    if let Some(refusal) = foreign_collisions(&jobs_commands, &existing, &to_delete) {
        return Ok(refusal);
    }
    // Jobs a root compose started are managed from there.
    let skipped = ctx.managed_elsewhere(project, &existing, &to_delete);
    let to_delete: Vec<String> = to_delete
        .into_iter()
        .filter(|n| !skipped.contains(n))
        .collect();
    jobs_commands.retain(|name, _| !skipped.contains(name));

    for job_name in &to_delete {
        // A local job the project's systemd executor takes over is not deleted: starting it
        // again moves it to systemd (stopping it first) and keeps its logs.
        if jobs_commands.contains_key(job_name) && ctx.jobs.moves_to_systemd(job_name) {
            ctx.out.messages.info(format!(
                "Moving job '{job_name}' to systemd so it survives a reboot..."
            ));
            continue;
        }
        let r = ctx.run(
            &["delete".into(), "job".into(), job_name.clone(), "-f".into()],
            project,
            RunOptions {
                capture_output: !ctx.args.debug,
                replace: false,
                ..RunOptions::default()
            },
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to delete job '{job_name}':\n{}",
                r.message
            )));
        }
    }

    // Several jobs (or several projects) starting together share the startup gate.
    // Each job is started by its own command, but they are one bulk start for the gate.
    ctx.bulk_start = !jobs_commands.is_empty();
    for (job_name, command) in &jobs_commands {
        ctx.out
            .messages
            .info(format!("Starting job '{job_name}'..."));
        let r = ctx.run(
            command,
            project,
            RunOptions {
                replace: false,
                ..RunOptions::default()
            },
        );
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                ctx.bulk_start = false;
                return Err(e);
            }
        };
        if !r.success {
            ctx.bulk_start = false;
            return Ok(SuccessTuple::fail(format!(
                "Failed to start job '{job_name}':\n{}",
                r.message
            )));
        }
    }

    ctx.bulk_start = false;
    if mode.force {
        let mut a = vec!["show".to_string(), "logs".to_string()];
        a.extend(jobs_commands.keys().cloned());
        ctx.run(
            &a,
            project,
            RunOptions {
                replace: false,
                ..RunOptions::default()
            },
        )?;
    }

    let mut msg = if explicit_jobs {
        format!(
            "Running {} background job{}.",
            jobs_commands.len(),
            plural(jobs_commands.len())
        )
    } else if pipes.len() == 1 {
        format!("Syncing {} in a background job.", pipes[0])
    } else {
        format!(
            "Syncing {} pipe{}{}{} instance{}.",
            pipes.len(),
            plural(pipes.len()),
            if jobs_commands.len() != 1 {
                " across "
            } else {
                " on "
            },
            jobs_commands.len(),
            plural(jobs_commands.len())
        )
    };
    if !mode.force {
        msg.push_str("\nRun `mrsm compose logs` or pass `-f` to follow logs output.");
    }
    Ok(SuccessTuple::ok(msg))
}

/// The refusal of `up` when a job it would start has the name of a job this project does not
/// own (`owned`: the existing jobs that belong to the project).
fn foreign_collisions(
    jobs_commands: &IndexMap<String, Vec<String>>,
    existing: &IndexMap<String, JobInfo>,
    owned: &[String],
) -> Option<SuccessTuple> {
    let collisions: Vec<&str> = jobs_commands
        .keys()
        .filter(|n| existing.contains_key(*n) && !owned.contains(n))
        .map(String::as_str)
        .collect();
    if collisions.is_empty() {
        return None;
    }
    Some(SuccessTuple::fail(format!(
        "Refusing to replace job{} not owned by this Compose project: {}\n\
         Nothing was registered, synced or started. Rename the job{} in this compose file, or \
         stop and delete {} first.",
        plural(collisions.len()),
        collisions.join(", "),
        plural(collisions.len()),
        if collisions.len() == 1 { "it" } else { "them" },
    )))
}

/// `run_initial_syncs(pipes, compose_config, sysargs)`: two passes of syncing.
fn run_initial_syncs(
    ctx: &mut Ctx,
    project: &Project,
    pipes: &[Pipe],
) -> Result<SuccessTuple, ComposeError> {
    const FLAGS_TO_REMOVE: &[&str] = &[
        "-c",
        "-C",
        "--connector-keys",
        "-m",
        "-M",
        "--metric-keys",
        "-l",
        "-L",
        "--location-keys",
        "-i",
        "-I",
        "--mrsm-instance",
        "--instance",
    ];
    let sysargs = &ctx.args.sysargs;
    let removed: Vec<usize> = sysargs
        .iter()
        .enumerate()
        .filter(|(_, f)| FLAGS_TO_REMOVE.contains(&f.as_str()))
        .map(|(i, _)| i)
        .collect();
    // PARITY: every other argument is forwarded, including the `compose` action words
    // (`sync pipes -c ... compose run --no-daemon`), as in up.py:367.
    let mut flags: Vec<String> = sysargs
        .iter()
        .enumerate()
        .filter(|(i, _)| !removed.contains(i) && !(*i > 0 && removed.contains(&(i - 1))))
        .map(|(_, f)| f.clone())
        .collect();
    if !flags.iter().any(|f| f == "--no-daemon") {
        flags.push("--no-daemon".into());
    }

    let sync_one = |ctx: &mut Ctx, pipe: &Pipe| -> Result<SuccessTuple, ComposeError> {
        if pipe.temporary {
            return Ok(ctx.instance.sync_pipe(pipe));
        }
        let mut a: Vec<String> = vec![
            "sync".into(),
            "pipes".into(),
            "-c".into(),
            pipe.connector_keys().into(),
            "-m".into(),
            pipe.metric_key().into(),
            "-l".into(),
            location_arg(pipe),
            "-i".into(),
            pipe.instance_keys().into(),
        ];
        a.extend(flags.iter().cloned());
        ctx.run(
            &a,
            project,
            RunOptions {
                replace: false,
                ..RunOptions::default()
            },
        )
    };

    let mut failed = Vec::new();
    for pipe in pipes {
        ctx.out.messages.info(format!("Syncing {pipe}..."));
        let r = sync_one(ctx, pipe)?;
        if !r.success {
            ctx.out
                .messages
                .warn(format!("Failed to sync {pipe}:\n{}", r.message));
            failed.push(pipe.clone());
        }
    }
    if failed.is_empty() {
        return Ok(SuccessTuple::success());
    }
    for pipe in &failed {
        ctx.out.messages.info(format!("Retry syncing {pipe}..."));
        let r = sync_one(ctx, pipe)?;
        if !r.success {
            ctx.out
                .messages
                .warn(format!("Failed to sync {pipe}:\n{}", r.message));
            return Ok(SuccessTuple::fail(format!(
                "Unable to begin syncing {pipe}:\n{}",
                r.message
            )));
        }
    }
    Ok(SuccessTuple::success())
}

/// `compose down [-v]`.
fn compose_down(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
) -> Result<SuccessTuple, ComposeError> {
    let project_name = project.project_name();
    let jobs = match ctx.project_jobs() {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let owned = get_project_job_names(&project.config, &[], &jobs, &mut ctx.out.messages)?;
    let skipped = ctx.managed_elsewhere(project, &jobs, &owned);
    for job_name in owned.into_iter().filter(|n| !skipped.contains(n)) {
        let r = ctx.run(
            &["delete".into(), "job".into(), job_name.clone(), "-f".into()],
            project,
            RunOptions::default(),
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to delete job '{job_name}':\n{}",
                r.message
            )));
        }
    }
    if !ctx.args.drop {
        return Ok(SuccessTuple::success());
    }

    build_custom_connectors(ctx, project)?;
    let all = defined_pipes(ctx, project, pc)?;
    let pipes: Vec<Pipe> = all
        .into_iter()
        .filter(|p| ctx.instance.get_id(p).is_some())
        .collect();
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to delete."));
    }
    let groups = instance_pipes(&pipes);
    let question = format!(
        "Remove {} pipe{} from this project? Unshared pipes and their data will be deleted{}.",
        pipes.len(),
        plural(pipes.len()),
        if pipes.len() > 1 {
            if groups.len() == 1 {
                " on 1 instance".to_string()
            } else {
                format!(" across {} instances", groups.len())
            }
        } else {
            String::new()
        }
    );
    if !ctx.yes_no(&question, false, false)? {
        return Ok(SuccessTuple::ok("Nothing was deleted."));
    }

    let mut failures: Vec<String> = Vec::new();
    for pipe in &pipes {
        let attrs = match ctx.instance.get_pipe_attributes(pipe) {
            Ok(a) => a,
            Err(e) => {
                failures.push(format!("Could not verify ownership of {pipe}: {e}"));
                continue;
            }
        };
        let remote_params = attrs.get("parameters").cloned();
        let remote_tags = match &remote_params {
            Some(Value::Map(m)) if !m.is_empty() => m.get("tags").cloned(),
            _ => None,
        };
        let (Some(Value::Map(remote_params)), Some(Value::List(remote_tags))) =
            (remote_params, remote_tags)
        else {
            failures.push(format!(
                "Could not verify ownership of {pipe}; it was preserved."
            ));
            continue;
        };
        let remaining: Vec<Value> = remote_tags
            .iter()
            .filter(|t| py_str(t) != project_name)
            .cloned()
            .collect();
        if remaining.len() == remote_tags.len() {
            ctx.out.messages.info(format!(
                "Preserving {pipe}; it is not tagged as '{project_name}'."
            ));
            continue;
        }
        let mut pipe = pipe.clone();
        pipe.set_parameters(remote_params.clone());
        if !remaining.is_empty() {
            ctx.out.messages.info(format!(
                "Preserving shared {pipe} and removing tag '{project_name}'."
            ));
            let mut params = remote_params;
            params.insert("tags".into(), Value::List(remaining));
            pipe.set_parameters(params);
            let r = ctx.instance.edit_pipe(&pipe);
            if !r.success {
                failures.push(format!("Failed to untag {pipe}: {}", r.message));
            }
            continue;
        }
        ctx.out.messages.info(format!("Deleting unshared {pipe}."));
        let r = ctx.instance.delete_pipe(&pipe);
        for warning in ctx.instance.take_warnings() {
            ctx.out.messages.warn(warning);
        }
        if !r.success {
            failures.push(format!("Failed to delete {pipe}: {}", r.message));
        }
    }
    if !failures.is_empty() {
        return Ok(SuccessTuple::fail(failures.join("\n")));
    }
    Ok(SuccessTuple::success())
}

/// The registration status shown by `explain`.
fn registration_status(
    temporary: bool,
    registered: bool,
    local: &Map,
    remote: &Map,
) -> (&'static str, bool) {
    if temporary {
        return ("🔳 Temporary", false);
    }
    if !registered {
        return ("⭕ Not registered", false);
    }
    let local_str = json_sorted_compact(&Value::Map(local.clone()));
    let remote_str = json_sorted_compact(&Value::Map(remote.clone()));
    if local_str == remote_str {
        return ("✅ Up-to-date", false);
    }
    // `{**local, **remote} != remote`: local keys missing from the remote parameters.
    let mut merged = local.clone();
    for (k, v) in remote {
        merged.insert(k.clone(), v.clone());
    }
    if py_eq(&Value::Map(merged), &Value::Map(remote.clone())) {
        ("🟨 Params added", true)
    } else {
        ("❌ Outdated", true)
    }
}

fn json_rich(params: &Map) -> String {
    json_dumps(&Value::Map(params.clone()), JsonStyle::Indent(2), false)
}

/// `compose explain`: the defined pipes and their registration status, plus the lint.
///
/// Side-effect free: nothing is created or installed, the file is not cached and no plugin is
/// fetched. An instance that cannot be reached, a connector that is not defined and a plugin
/// that is not installed are findings, and the table is still printed.
fn compose_explain(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
    tree_pipes: &[PipeKey],
) -> Result<SuccessTuple, ComposeError> {
    let defaults = pc.pipe_defaults();
    let default_instance = project
        .config_block()
        .get("meerschaum")
        .and_then(Value::as_map)
        .and_then(|m| m.get("instance"))
        .map(py_str)
        .unwrap_or_else(|| defaults.instance_keys.clone());
    let mut findings = lint(
        &LintInput {
            document: &project.document,
            compose_file: &project.file,
            defaults: &defaults,
            default_instance: &default_instance,
            also_defined: tree_pipes,
        },
        ctx.instance,
    );

    let project_name = project.project_name();
    // The lint is reported even when the pipes cannot be built.
    if ctx.multi {
        for f in &mut findings {
            f.location = format!("{project_name}: {}", f.location);
        }
    }
    ctx.out.lint.append(&mut findings);
    build_custom_connectors(ctx, project)?;
    let pipes = defined_pipes(ctx, project, pc)?;
    let mut findings = project_findings(ctx, project, pc);
    if ctx.multi {
        for f in &mut findings {
            f.location = format!("{project_name}: {}", f.location);
        }
    }
    let groups = instance_pipes(&pipes);
    let tags = vec![project_name.clone()];
    let mut remote: IndexMap<String, Vec<Pipe>> = IndexMap::new();
    let mut unreachable: IndexMap<String, String> = IndexMap::new();
    for inst in groups.keys() {
        match ctx.instance.try_get_pipes(inst, &tags) {
            Ok(r) => {
                remote.insert(inst.clone(), r);
            }
            Err(e) => {
                findings.push(LintWarning::warning(
                    "unreachable-instance",
                    format!("{project_name}: {inst}"),
                    format!(
                        "The instance '{inst}' cannot be reached, so registrations are unknown: {}",
                        e.trim()
                    ),
                ));
                unreachable.insert(inst.clone(), e);
            }
        }
    }
    ctx.out.lint.extend(findings);

    let width = ctx.args.width.unwrap_or(80);
    let mut model = ExplainModel {
        project_name: project_name.clone(),
        width,
        default_instance: default_instance.clone(),
        instances: Vec::new(),
    };
    let mut rows: Vec<(Vec<Renderable>, bool, bool)> = Vec::new();
    for (inst, idxs) in &groups {
        rows.push((
            vec![
                Renderable::Panel {
                    body: inst.clone(),
                    title: "Instance".into(),
                },
                Renderable::text(""),
                Renderable::text(""),
            ],
            false,
            true,
        ));
        let mut explained = ExplainInstance {
            keys: inst.clone(),
            pipes: Vec::new(),
        };
        for (i, ix) in idxs.iter().enumerate() {
            let pipe = &pipes[*ix];
            let is_unreachable = unreachable.contains_key(inst);
            let registered = remote.get(inst).is_some_and(|rs| {
                rs.iter().any(|r| {
                    r.connector_keys() == pipe.connector_keys()
                        && r.metric_key() == pipe.metric_key()
                        && r.location_key() == pipe.location_key()
                })
            });
            let remote_params = if is_unreachable {
                Map::new()
            } else {
                ctx.instance.get_parameters(pipe).unwrap_or_default()
            };
            let local_params = pipe.raw_parameters().clone();
            let (status, include_remote) = if is_unreachable && !pipe.temporary {
                ("❔ Unknown (instance unreachable)", false)
            } else {
                registration_status(pipe.temporary, registered, &local_params, &remote_params)
            };
            let end_section = i == idxs.len() - 1;
            rows.push((
                vec![
                    Renderable::text(format!("{pipe}\n\n{status}\n")),
                    Renderable::json(json_rich(&local_params)),
                    if include_remote {
                        Renderable::json(json_rich(&remote_params))
                    } else {
                        Renderable::text("")
                    },
                ],
                end_section,
                true,
            ));
            explained.pipes.push(ExplainPipe {
                connector: pipe.connector_keys().to_string(),
                metric: pipe.metric_key().to_string(),
                location: pipe.location_key().map(String::from),
                instance: pipe.instance_keys().to_string(),
                status: status.to_string(),
                local: local_params,
                remote: include_remote.then_some(remote_params),
            });
        }
        model.instances.push(explained);
    }

    // PARITY: the instance rows carry an empty remote cell (not `None`), so the "Remote
    // Parameters" column is shown whenever there is at least one pipe (explain.py:249).
    let include_remote_col = rows.iter().any(|(_, _, has_remote)| *has_remote);
    let mut headers = vec![
        "Defined Pipes".to_string(),
        "Compose Parameters".to_string(),
    ];
    if include_remote_col {
        headers.push("Remote Parameters".into());
    }
    let mut table_rows = Vec::new();
    for (mut cells, end_section, _) in rows {
        if !include_remote_col {
            cells.truncate(2);
        }
        table_rows.push(Row { cells, end_section });
        if !end_section {
            table_rows.push(Row {
                cells: vec![Renderable::text(""), Renderable::text("")],
                end_section: false,
            });
        }
    }
    let table = Table {
        title: format!("Pipes in '{project_name}'"),
        headers,
        rows: table_rows,
    };
    let rendered = table.render(width);
    match &mut ctx.out.explain {
        Some(existing) => {
            existing.push('\n');
            existing.push_str(&rendered);
        }
        none => *none = Some(rendered),
    }
    ctx.out.explain_models.push(model);
    Ok(SuccessTuple::success())
}

/// What `_scope_job_action` decided.
enum Scoped {
    /// Run these sysargs.
    Run(Vec<String>),
    /// Nothing to run (or refused).
    Done(SuccessTuple),
}

/// The flags compose reads itself and the action must not see.
const COMPOSE_VALUE_FLAGS: &[&str] = &["--file", "--env-file", "--project"];

/// `_compose_default`: run any other action inside the project.
fn compose_default(
    ctx: &mut Ctx,
    project: &Project,
    run: &RunTree,
    ix: usize,
) -> Result<SuccessTuple, ComposeError> {
    let project_name = project.project_name();
    let mut isolated: Vec<String> = Vec::new();
    let mut skip_next = false;
    // PARITY: only `--file` and `--env-file` (not their aliases) are stripped
    // (default.py:30). The v5 flags `--project`, `--standalone` and `--no-stagger` are compose's.
    for arg in ctx.args.sysargs.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if COMPOSE_VALUE_FLAGS.contains(&arg.as_str()) {
            skip_next = true;
            continue;
        }
        if arg.starts_with("--project=") || arg == "--standalone" || arg == "--no-stagger" {
            continue;
        }
        isolated.push(arg.clone());
    }
    let action = ctx.args.action.clone();
    let sysargs = match scope_job_action(ctx, project, &action, isolated, run, ix)? {
        Scoped::Run(s) => s,
        Scoped::Done(result) => return Ok(result),
    };
    let subprocess = project.config.get("isolation").and_then(Value::as_str) == Some("subprocess")
        || action.is_empty()
        || ["install", "setup"].contains(&action[0].as_str());
    if action.is_empty() {
        ctx.out.messages.info(format!(
            "Starting a shell in compose project '{project_name}'..."
        ));
    } else {
        ctx.out.messages.info(format!(
            "Running '{}' in compose project '{project_name}'...",
            action.join(" ")
        ));
    }
    // Syncing against a host connector that is not defined fails here, naming it.
    if action.first().is_some_and(|a| a == "sync" || a == "verify") {
        let pc = ProjectConfig::new(ctx.host, project)?;
        if let Some(message) = refs::blocking_message(&refs::findings(project, &pc, &[], &[])) {
            ctx.bulk_start = false;
            return Ok(SuccessTuple::fail(message));
        }
    }
    let result = ctx.run(
        &sysargs,
        project,
        RunOptions {
            subprocess: Some(subprocess),
            replace: false,
            ..RunOptions::default()
        },
    );
    ctx.bulk_start = false;
    result
}

fn filter_status(jobs: &IndexMap<String, JobInfo>, f: impl Fn(&JobInfo) -> bool) -> Vec<String> {
    jobs.iter()
        .filter(|(_, j)| f(j))
        .map(|(n, _)| n.clone())
        .collect()
}

/// The project of the tree each named job belongs to: the project that owns the existing job,
/// else the project that configures it, else the primary project. A name owned by a project
/// outside the tree stays with the primary project (which refuses it).
fn route_job_names(
    ctx: &mut Ctx,
    run: &RunTree,
    jobs: &IndexMap<String, JobInfo>,
    names: &[String],
) -> Result<IndexMap<String, usize>, ComposeError> {
    let mut configured: Vec<(usize, Vec<String>)> = Vec::new();
    for ix in &run.selected {
        let project = &run.tree.nodes[*ix].project;
        let keys = match get_jobs_commands(&project.config, &[], &mut ctx.out.messages) {
            Ok(commands) => commands.into_keys().collect(),
            Err(_) => Vec::new(),
        };
        configured.push((*ix, keys));
    }
    let mut routes = IndexMap::new();
    for name in names {
        let owner = jobs.get(name).and_then(job_project_name).and_then(|p| {
            run.selected
                .iter()
                .copied()
                .find(|ix| run.tree.nodes[*ix].project.project_name() == p)
        });
        let target = owner
            .or_else(|| {
                configured
                    .iter()
                    .find(|(_, keys)| keys.contains(name))
                    .map(|(ix, _)| *ix)
            })
            .unwrap_or(run.primary);
        routes.insert(name.clone(), target);
    }
    Ok(routes)
}

fn scope_job_action(
    ctx: &mut Ctx,
    project: &Project,
    action: &[String],
    sysargs: Vec<String>,
    run: &RunTree,
    node: usize,
) -> Result<Scoped, ComposeError> {
    if action.len() < 2
        || !SCOPED_JOB_ACTIONS.contains(&action[0].as_str())
        || (action[1] != "job" && action[1] != "jobs")
    {
        return Ok(Scoped::Run(sysargs));
    }
    let verb = action[0].as_str();
    let project_name = project.project_name();
    let jobs = match ctx.project_jobs() {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(Scoped::Done(failed)),
    };
    let mut job_names: Vec<String> = action[2..].to_vec();
    let mut sysargs = sysargs;
    if !job_names.is_empty() {
        if ctx.multi {
            // Several projects share the command: each takes the jobs that are its own.
            let routes = route_job_names(ctx, run, &jobs, &job_names)?;
            let mine: Vec<String> = job_names
                .iter()
                .filter(|n| routes.get(*n) == Some(&node))
                .cloned()
                .collect();
            if mine.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(format!(
                    "None of the named jobs belong to compose project '{project_name}'."
                ))));
            }
            if mine.len() != job_names.len() {
                // Keep only this project's names in the command.
                for ix in 0..sysargs.len().saturating_sub(1) {
                    if sysargs[ix..ix + 2] == action[..2]
                        && sysargs.len() >= ix + 2 + job_names.len()
                        && sysargs[ix + 2..ix + 2 + job_names.len()] == job_names[..]
                    {
                        let mut out: Vec<String> = sysargs[..ix + 2].to_vec();
                        out.extend(mine.iter().cloned());
                        out.extend(sysargs[ix + 2 + job_names.len()..].iter().cloned());
                        sysargs = out;
                        break;
                    }
                }
            }
            job_names = mine;
        }
        let foreign: Vec<&String> = job_names
            .iter()
            .filter(|n| {
                jobs.get(*n)
                    .is_some_and(|j| job_project_name(j).is_some_and(|p| p != project_name))
            })
            .collect();
        if !foreign.is_empty() {
            return Ok(Scoped::Done(SuccessTuple::fail(format!(
                "Refusing to {verb} job{} owned by another Compose project: {}\nRun `{verb} jobs` from the owning project's compose file.",
                plural(foreign.len()),
                foreign
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))));
        }
        let skipped = ctx.managed_elsewhere(project, &jobs, &job_names);
        if !skipped.is_empty() {
            let remaining: Vec<String> = job_names
                .iter()
                .filter(|n| !skipped.contains(n))
                .cloned()
                .collect();
            if remaining.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(format!(
                    "No jobs to {verb} in compose project '{project_name}'."
                ))));
            }
            for ix in 0..sysargs.len().saturating_sub(1) {
                if sysargs[ix..ix + 2] == action[..2]
                    && sysargs.len() >= ix + 2 + job_names.len()
                    && sysargs[ix + 2..ix + 2 + job_names.len()] == job_names[..]
                {
                    let mut out: Vec<String> = sysargs[..ix + 2].to_vec();
                    out.extend(remaining.iter().cloned());
                    out.extend(sysargs[ix + 2 + job_names.len()..].iter().cloned());
                    sysargs = out;
                    break;
                }
            }
        }
        return Ok(Scoped::Run(sysargs));
    }

    let names = get_project_job_names(&project.config, &[], &jobs, &mut ctx.out.messages)?;
    let skipped = ctx.managed_elsewhere(project, &jobs, &names);
    let project_jobs: IndexMap<String, JobInfo> = names
        .iter()
        .filter(|n| !skipped.contains(n))
        .filter_map(|n| jobs.get(n).map(|j| (n.clone(), j.clone())))
        .collect();
    if project_jobs.is_empty() {
        return Ok(Scoped::Done(SuccessTuple::ok(format!(
            "No jobs in compose project '{project_name}'."
        ))));
    }
    let mut to_act: Vec<String> = Vec::new();
    let add = |names: Vec<String>, to_act: &mut Vec<String>| {
        for n in names {
            if !to_act.contains(&n) {
                to_act.push(n);
            }
        }
    };
    match verb {
        "start" => {
            add(
                filter_status(&project_jobs, |j| j.status == "stopped"),
                &mut to_act,
            );
            add(
                filter_status(&project_jobs, |j| j.status == "paused"),
                &mut to_act,
            );
            if to_act.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok("All jobs are running.")));
            }
        }
        "restart" => {
            add(filter_status(&project_jobs, |j| j.restart), &mut to_act);
            if to_act.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(
                    "No jobs need to be restarted.",
                )));
            }
        }
        "stop" | "pause" => {
            add(
                filter_status(&project_jobs, |j| j.status == "running"),
                &mut to_act,
            );
            add(
                filter_status(&project_jobs, |j| j.status == "paused"),
                &mut to_act,
            );
            if verb == "stop" {
                add(filter_status(&project_jobs, |j| j.restart), &mut to_act);
            }
            if to_act.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(format!(
                    "No jobs to {verb} in compose project '{project_name}'."
                ))));
            }
        }
        _ => to_act = project_jobs.keys().cloned().collect(),
    }
    let project_job_names: Vec<String> = project_jobs
        .keys()
        .filter(|n| to_act.contains(n))
        .cloned()
        .collect();

    if (verb == "stop" || verb == "pause") && !ctx.args.force {
        let mut capitalized = verb.to_string();
        capitalized[..1].make_ascii_uppercase();
        let noask = ctx.args.noask;
        if !ctx.yes_no(&format!("{capitalized} the above jobs?"), false, noask)? {
            return Ok(Scoped::Done(SuccessTuple::fail(format!(
                "No jobs were {}.",
                if verb == "stop" { "stopped" } else { "paused" }
            ))));
        }
    }

    // `start jobs` and `restart jobs` over the project's jobs are bulk starts.
    if verb == "start" || verb == "restart" {
        ctx.bulk_start = true;
    }
    for ix in 0..sysargs.len().saturating_sub(1) {
        if sysargs[ix..ix + 2] == action[..2] {
            let mut out: Vec<String> = sysargs[..ix + 2].to_vec();
            out.extend(project_job_names.iter().cloned());
            out.extend(sysargs[ix + 2..].iter().cloned());
            return Ok(Scoped::Run(out));
        }
    }
    Ok(Scoped::Done(SuccessTuple::fail(format!(
        "Could not scope `{}` to compose project '{project_name}'.\nName the jobs explicitly.",
        action.join(" ")
    ))))
}

/// The compose file `compose init` writes for a new project (`_write_new_compose_file`).
pub fn new_compose_file_text(project_name: &str, pipes: &[Map]) -> String {
    let mut doc = Map::new();
    doc.insert("project_name".into(), Value::Str(project_name.into()));
    doc.insert("root_dir".into(), Value::Str("./root".into()));
    doc.insert(
        "plugins_dir".into(),
        Value::List(vec![Value::Str("./plugins".into())]),
    );
    doc.insert(
        "pipes".into(),
        Value::List(pipes.iter().cloned().map(Value::Map).collect()),
    );
    doc.insert("plugins".into(), Value::List(Vec::new()));
    let mut sql = Map::new();
    sql.insert(
        "main".into(),
        Value::Str("MRSM{meerschaum:connectors:sql:main}".into()),
    );
    let mut connectors = Map::new();
    connectors.insert("sql".into(), Value::Map(sql));
    let mut mrsm = Map::new();
    mrsm.insert("connectors".into(), Value::Map(connectors));
    let mut config = Map::new();
    config.insert("meerschaum".into(), Value::Map(mrsm));
    doc.insert("config".into(), Value::Map(config));
    doc.insert("environment".into(), Value::new_map());
    format!(
        "### See https://meerschaum.io/reference/compose/ for the compose schema.\n\n{}",
        meerschaum_core::yaml::dump(&Value::Map(doc))
            .expect("the compose template is a fixed document the YAML emitter accepts")
    )
}

/// The `pipes:` entry `compose init` writes for a registered pipe.
fn pipe_entry(pipe: &Pipe) -> Map {
    let mut entry = Map::new();
    entry.insert("connector".into(), Value::Str(pipe.connector_keys().into()));
    entry.insert("metric".into(), Value::Str(pipe.metric_key().into()));
    entry.insert(
        "location".into(),
        pipe.location_key()
            .map_or(Value::Null, |l| Value::Str(l.into())),
    );
    entry.insert("instance".into(), Value::Str(pipe.instance_keys().into()));
    entry.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    entry
}

/// `compose init`: create a project (if none exists) and install its plugins' requirements.
fn compose_init(ctx: &mut Ctx) -> Result<SuccessTuple, ComposeError> {
    let mut file = ctx.args.file.clone();
    let found = infer_compose_file_path(file.as_deref(), &ctx.host.cwd, &mut ctx.out.messages);
    if found.is_none() {
        let cwd = ctx.host.cwd.clone();
        let compose_path = cwd.join("mrsm-compose.yaml");
        file = Some(compose_path.clone());
        ctx.out.messages.info("No compose project could be found.");
        let dir_name = cwd
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let create = ctx
            .yes_no(
                &format!(
                    "Create new project '{dir_name}'?\n    ({})",
                    compose_path.display()
                ),
                true,
                false,
            )
            .unwrap_or(false);
        if !create {
            return Ok(SuccessTuple::fail("No files were created."));
        }
        // `-c/-m/-l/-t/-i` add the matching registered pipes.
        let filters = PipeFilters {
            connector_keys: ctx.args.connector_keys.clone(),
            metric_keys: ctx.args.metric_keys.clone(),
            location_keys: ctx.args.location_keys.clone(),
            tags: ctx.args.tags.clone(),
            instance: ctx.args.mrsm_instance.clone(),
        };
        let mut entries: Vec<Map> = Vec::new();
        if filters.is_empty() {
            ctx.out
                .messages
                .info("Run `compose init` with filters (-c, -m, -l, -t, -i) to add pipes.");
        } else if ctx.yes_no(
            "Add pipes from the given filters to the compose file?",
            true,
            false,
        )? {
            ctx.instance.configure_host(ctx.host);
            let pipes = ctx
                .instance
                .get_pipes_matching(&filters)
                .map_err(|e| ComposeError::new(ErrorKind::Exception, e))?;
            ctx.out.messages.info(if pipes.is_empty() {
                "No pipes match the given filters.".to_string()
            } else {
                format!(
                    "Pipes matching the given filters:\n{}",
                    pipes
                        .iter()
                        .map(|p| format!("  {p}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            });
            if ctx.yes_no("Add these pipes to the compose file?", true, false)? {
                entries = pipes.iter().map(pipe_entry).collect();
            } else {
                ctx.out.messages.info("No pipes will be added.");
            }
        }
        if compose_path.exists() {
            return Err(ComposeError::new(
                ErrorKind::FileExists,
                format!("File '{}' already exists.", compose_path.display()),
            ));
        }
        let text = new_compose_file_text(&dir_name, &entries);
        if std::fs::write(&compose_path, &text).is_err() {
            return Ok(SuccessTuple::fail(format!(
                "Failed to create file '{}'.",
                compose_path.display()
            )));
        }
        ctx.out.written_files.push((compose_path, text));
    }

    let run = load_run_tree(ctx, file.as_deref(), false, true)?;
    ctx.multi = run.selected.len() > 1;
    let mut results: Vec<(String, SuccessTuple)> = Vec::new();
    for ix in &run.selected {
        let project = run.tree.nodes[*ix].project.clone();
        ctx.primary = *ix == run.primary;
        let result = init_project(ctx, &project)?;
        let ok = result.success;
        results.push((project.project_name(), result));
        if !ok {
            break;
        }
    }
    Ok(combine_results(results, ctx.multi))
}

/// `compose init` for one project of the run.
fn init_project(ctx: &mut Ctx, project: &Project) -> Result<SuccessTuple, ComposeError> {
    init_root(ctx, project)?;
    let pc = ProjectConfig::new(ctx.host, project)?;
    let project_name = project.project_name();
    let existing = get_installed_plugins(project)?;
    let r = check_and_install_plugins(
        project,
        &pc,
        Some(existing.clone()),
        ctx.args.debug,
        &mut ctx.runner,
        &mut ctx.out.messages,
    )?;
    if !r.success {
        return Ok(r);
    }
    let installed = if existing.is_empty() {
        get_installed_plugins(project)?
    } else {
        existing
    };
    if !installed.is_empty() {
        let r = ctx.run(
            &["install".into(), "required".into()],
            project,
            RunOptions {
                subprocess: Some(true),
                ..RunOptions::default()
            },
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to install required packages for project '{project_name}':\n{}",
                r.message
            )));
        }
        let r = ctx.run(
            &["setup".into(), "plugins".into()],
            project,
            RunOptions {
                subprocess: Some(true),
                ..RunOptions::default()
            },
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to setup plugins for project '{project_name}':\n{}",
                r.message
            )));
        }
    }
    Ok(SuccessTuple::ok(format!(
        "Finished initializing project '{project_name}'."
    )))
}

/// The commands a subaction would run, without running anything (every command reports
/// success). Used for `--dry` previews.
pub fn dry_commands(
    host: &Host,
    session: &mut Session,
    args: &ComposeArgs,
    instance: &mut dyn InstanceOps,
    jobs: &mut dyn JobOps,
    prompter: &mut dyn Prompter,
) -> ComposeOutput {
    let mut runner = crate::command::RecordingRunner::default();
    compose(
        host,
        session,
        args,
        Services {
            runner: &mut runner,
            instance,
            jobs,
            prompter,
        },
    )
}

/// Build the command for `args` in the project without running it (for callers that want to
/// show the exact command line).
pub fn describe_command(
    args: &[String],
    project: &Project,
    opts: RunOptions,
) -> Result<MrsmCommand, ComposeError> {
    build_command(args, project, opts, &mut Messages::default())
}

/// Run a built command (re-exported for callers that build commands themselves).
pub fn run_command(command: &MrsmCommand, runner: &mut dyn CommandRunner) -> SuccessTuple {
    execute(command, runner)
}

/// The compose file a `--file` argument (or the defaults) resolves to.
pub fn locate(file: Option<&Path>, cwd: &Path) -> (Option<PathBuf>, Messages) {
    let mut msgs = Messages::default();
    let found = infer_compose_file_path(file, cwd, &mut msgs);
    (found, msgs)
}
