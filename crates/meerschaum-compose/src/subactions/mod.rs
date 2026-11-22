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
use crate::jobs::{
    get_jobs_commands, get_project_job_names, job_compose_root, job_project_name, jobs_disabled,
};
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

mod default_action;
mod down;
mod explain;
mod init;
mod up;

use default_action::compose_default;
use down::compose_down;
use explain::compose_explain;
use init::compose_init;
pub use init::new_compose_file_text;
use up::compose_up;

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
    /// Exact named selections declared by this project.
    pub pipelines: Vec<crate::pipelines::PipelineSpec>,
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
    pipeline_revisions: std::collections::BTreeMap<(String, String, String), Option<u64>>,
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
        pipeline_revisions: Default::default(),
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
    if matches!(selected, "up" | "run" | "explain") {
        let mut identities = std::collections::BTreeMap::new();
        for ix in &run.selected {
            let project = &run.tree.nodes[*ix].project;
            let pc = ProjectConfig::new(ctx.host, project)?;
            let definitions = crate::pipelines::defined(project, &pc)?;
            for spec in &definitions {
                if let Some(previous) = identities.insert(
                    (spec.instance.clone(), spec.name.clone()),
                    project.project_name(),
                ) {
                    return Err(ComposeError::value(format!(
                        "Pipeline '{}' on '{}' is declared by both '{previous}' and '{}'.",
                        spec.name,
                        spec.instance,
                        project.project_name()
                    )));
                }
            }
            if selected != "explain" && !definitions.is_empty() {
                ctx.instance
                    .try_configure(ctx.host, project)
                    .map_err(ComposeError::value)?;
                for spec in definitions {
                    let previous = ctx
                        .instance
                        .pipeline_parameters(&spec)
                        .map_err(ComposeError::value)?;
                    let revision = spec
                        .check_owner(previous.as_ref(), &project.project_name())
                        .map_err(ComposeError::value)?;
                    ctx.pipeline_revisions
                        .insert((project.project_name(), spec.instance, spec.name), revision);
                }
            }
        }
    }
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
