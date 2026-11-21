//! Compose trees (`include:` / `parent:`, design decision D22), missing references and the
//! side-effect-free `explain`, over synthetic projects, a fake instance, fake jobs and a
//! recording command runner.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use meerschaum_compose::command::RecordingRunner;
use meerschaum_compose::error::ErrorKind;
use meerschaum_compose::lint::Severity;
use meerschaum_compose::ops::{
    InMemoryInstance, InMemoryJobs, InstanceEvent, JobInfo, NoInput, RegisteredPipe,
    ScriptedPrompter,
};
use meerschaum_compose::project::{COMPOSE_ROOT_VAR, Host, Session};
use meerschaum_compose::subactions::{ComposeArgs, ComposeOutput, Services, compose};
use meerschaum_core::value::Value;

struct Sandbox {
    _tmp: tempfile::TempDir,
    base: PathBuf,
}

/// The files of a project tree under a fresh temporary directory.
fn sandbox(files: &[(&str, &str)]) -> Sandbox {
    let tmp = tempfile::tempdir().unwrap();
    let base = std::fs::canonicalize(tmp.path()).unwrap();
    for sub in ["host-root", "host-plugins", "home"] {
        std::fs::create_dir_all(base.join(sub)).unwrap();
    }
    for (path, text) in files {
        let file = base.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, text).unwrap();
    }
    Sandbox { _tmp: tmp, base }
}

impl Sandbox {
    fn host(&self, cwd: &str) -> Host {
        let mut vars = IndexMap::new();
        let p = |s: &str| self.base.join(s).to_string_lossy().to_string();
        vars.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
        vars.insert("HOME".to_string(), p("home"));
        vars.insert("MRSM_ROOT_DIR".to_string(), p("host-root"));
        vars.insert("MRSM_PLUGINS_DIR".to_string(), p("host-plugins"));
        Host::new(self.base.join(cwd), vars).unwrap()
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.base.join(rel)
    }
}

struct Run {
    out: ComposeOutput,
    commands: Vec<Vec<String>>,
    instance: InMemoryInstance,
}

impl Run {
    fn names(&self) -> Vec<String> {
        self.out.projects.iter().map(|p| p.project_name()).collect()
    }

    fn message(&self) -> String {
        match &self.out.result {
            Ok(t) => t.message.clone(),
            Err(e) => e.message.clone(),
        }
    }

    fn warnings(&self) -> Vec<String> {
        self.out
            .messages
            .warnings()
            .into_iter()
            .map(String::from)
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
fn go_with(
    sandbox: &Sandbox,
    cwd: &str,
    action: &[&str],
    instance: InMemoryInstance,
    jobs: &mut InMemoryJobs,
    prompter: &mut dyn meerschaum_compose::ops::Prompter,
    tweak: impl FnOnce(&mut ComposeArgs),
) -> Run {
    let host = sandbox.host(cwd);
    let mut sysargs = vec!["compose".to_string()];
    sysargs.extend(action.iter().map(|s| s.to_string()));
    let mut args = ComposeArgs {
        action: action.iter().map(|s| s.to_string()).collect(),
        sysargs,
        yes: true,
        width: Some(100),
        ..ComposeArgs::default()
    };
    tweak(&mut args);
    let mut runner = RecordingRunner::default();
    let mut instance = instance;
    let out = compose(
        &host,
        &mut Session::default(),
        &args,
        Services {
            runner: &mut runner,
            instance: &mut instance,
            jobs,
            prompter,
        },
    );
    Run {
        out,
        commands: runner.commands.into_iter().map(|c| c.sysargs).collect(),
        instance,
    }
}

fn go(sandbox: &Sandbox, cwd: &str, action: &[&str]) -> Run {
    go_with(
        sandbox,
        cwd,
        action,
        InMemoryInstance::default(),
        &mut InMemoryJobs::default(),
        &mut NoInput,
        |_| {},
    )
}

fn root_project(run: &Run) -> &meerschaum_compose::Project {
    run.out.project.as_ref().expect("a project was loaded")
}

fn project<'a>(run: &'a Run, name: &str) -> &'a meerschaum_compose::Project {
    run.out
        .projects
        .iter()
        .find(|p| p.project_name() == name)
        .unwrap_or_else(|| panic!("no project '{name}' in {:?}", run.names()))
}

fn str_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_list)
        .map(|l| l.iter().map(meerschaum_core::pyfmt::py_str).collect())
        .unwrap_or_default()
}

fn config_path<'a>(p: &'a meerschaum_compose::Project, keys: &[&str]) -> Option<&'a Value> {
    let mut cur = p.config.get(keys[0])?;
    for k in &keys[1..] {
        cur = cur.as_map()?.get(*k)?;
    }
    Some(cur)
}

const ROOT_WITH_INCLUDES: &str = "project_name: top\nroot_dir: ./root\ninclude:\n  - projects/\n";

// ---- include: forms, order and identity ------------------------------------------------------

#[test]
fn unnamed_includes_use_their_own_filenames_and_conventional_directory() {
    let s = sandbox(&[
        (
            "top/mrsm-compose.yaml",
            "include: [extract.yaml, load.yml, reporting.prod/]\n",
        ),
        ("top/extract.yaml", "pipes: []\n"),
        ("top/load.yml", "pipes: []\n"),
        ("top/reporting.prod/mrsm-compose.yaml", "pipes: []\n"),
    ]);
    let run = go(&s, "top", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    let mut names = run.names();
    names.sort();
    assert_eq!(names, vec!["extract", "load", "reporting.prod", "top"]);
}

#[test]
fn include_takes_files_directories_globs_and_optional_paths() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude:\n  - one.yaml\n  - projects/\n  - 'globbed/*.yml'\n  - 'nothing/*.yaml'\n  - path: gone.yaml\n    required: false\n",
        ),
        ("one.yaml", "project_name: one\n"),
        ("projects/b.yaml", "project_name: b\n"),
        ("projects/a.yml", "project_name: a\n"),
        (
            "projects/nested/mrsm-compose.yaml",
            "project_name: nested\n",
        ),
        ("projects/notes.txt", "not a compose file"),
        ("globbed/g.yml", "project_name: g\n"),
    ]);
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // Includes run before the root, depth-first, in the order listed; a directory is its
    // `*.yaml` / `*.yml` files, then `<dir>/*/mrsm-compose.yaml`.
    assert_eq!(run.names(), vec!["one", "a", "b", "nested", "g", "top"]);
    // The missing optional path warns, the glob that matched nothing is silent.
    let warnings = run.warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("gone.yaml") && warnings[0].contains("does not exist"));
}

#[test]
fn a_missing_required_include_is_an_error() {
    let s = sandbox(&[(
        "mrsm-compose.yaml",
        "project_name: top\nroot_dir: ./root\ninclude: [missing.yaml]\n",
    )]);
    let run = go(&s, "", &["explain"]);
    let err = run.out.result.as_ref().unwrap_err();
    assert_eq!(err.kind, ErrorKind::FileNotFound);
    assert!(err.message.contains("missing.yaml"), "{}", err.message);
}

#[test]
fn glob_entries_match_under_a_directory_with_glob_characters() {
    // The including file's own directory is not a pattern: `proj[1]` is not a character class.
    let s = sandbox(&[
        (
            "proj[1]/mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: ['sub*/mrsm-compose.yaml', 'clients/[acme]']\n",
        ),
        ("proj[1]/sub1/mrsm-compose.yaml", "project_name: one\n"),
        ("proj[1]/sub2/mrsm-compose.yaml", "project_name: two\n"),
        (
            "proj[1]/clients/[acme]/mrsm-compose.yaml",
            "project_name: acme\n",
        ),
    ]);
    let run = go(&s, "proj[1]", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // A literal directory with brackets is a path, not a character class; the glob still globs.
    assert_eq!(run.names(), vec!["one", "two", "acme", "top"]);
}

#[test]
fn duplicate_project_names_refuse_every_job_operation_and_explain_lists_them() {
    // Two sibling directories with the same basename: the default project name collides.
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [a/app, b/app]\n",
        ),
        ("a/app/mrsm-compose.yaml", "jobs:\n  aj: show version\n"),
        ("b/app/mrsm-compose.yaml", "jobs:\n  bj: show version\n"),
    ]);
    for action in ["up", "down", "run", "ps", "logs"] {
        let run = go(&s, "", &[action]);
        let message = run.message();
        assert!(
            !run.out.result.as_ref().map(|t| t.success).unwrap_or(false),
            "{action}: {message}"
        );
        assert!(
            message.contains("are both named 'app'") && message.contains("project_name"),
            "{action}: {message}"
        );
        // Nothing was started or deleted.
        assert!(run.commands.is_empty(), "{action}: {:?}", run.commands);
    }
    // `explain` reports it as a finding and keeps going.
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert!(
        run.out
            .lint
            .iter()
            .any(|f| f.code == "duplicate-project-name"),
        "{:?}",
        run.out.lint
    );
    // Naming one of them fixes it.
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [a/app, b/app]\n",
        ),
        ("a/app/mrsm-compose.yaml", "jobs:\n  aj: show version\n"),
        (
            "b/app/mrsm-compose.yaml",
            "project_name: app-b\njobs:\n  bj: show version\n",
        ),
    ]);
    let run = go(&s, "", &["up"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
}

#[test]
fn an_included_file_that_fails_to_parse_stops_down_and_names_it() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [sub.yaml]\njobs:\n  tj: show version\n",
        ),
        ("sub.yaml", "project_name: [unclosed\n  nope: : :\n"),
    ]);
    let mut jobs = InMemoryJobs::default();
    jobs.jobs.insert("tj".into(), owned_job("top", None));
    for action in ["down", "up", "ps"] {
        let run = go_with(
            &s,
            "",
            &[action],
            InMemoryInstance::default(),
            &mut jobs,
            &mut NoInput,
            |_| {},
        );
        let message = run.message();
        assert!(
            !run.out.result.as_ref().map(|t| t.success).unwrap_or(false),
            "{action}: {message}"
        );
        assert!(
            message.contains("sub.yaml") && message.contains("jobs were not touched"),
            "{action}: {message}"
        );
        assert!(run.commands.is_empty(), "{action}: {:?}", run.commands);
    }
    assert!(jobs.jobs.contains_key("tj"));
}

#[test]
fn includes_are_transitive_cycles_warn_and_a_file_loads_once() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [a.yaml, b.yaml]\n",
        ),
        // `a` includes `c`, and `c` includes the root again (a cycle).
        ("a.yaml", "project_name: a\ninclude: c.yaml\n"),
        ("c.yaml", "project_name: c\ninclude: mrsm-compose.yaml\n"),
        // `b` includes `c` too: it is already loaded.
        ("b.yaml", "project_name: b\ninclude: [c.yaml]\n"),
    ]);
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert_eq!(run.names(), vec!["c", "a", "b", "top"]);
    let warnings = run.warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("already being loaded above"),
        "{warnings:?}"
    );
}

#[test]
fn a_file_that_fails_to_parse_is_reported_and_skipped() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [broken.yaml, good.yaml]\n",
        ),
        ("broken.yaml", "project_name: [unclosed\n  nope: : :\n"),
        ("good.yaml", "project_name: good\n"),
    ]);
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert_eq!(run.names(), vec!["good", "top"]);
    assert!(
        run.warnings()
            .iter()
            .any(|w| w.contains("broken.yaml") && w.contains("Could not load")),
        "{:?}",
        run.warnings()
    );
}

#[test]
fn included_projects_keep_their_identity_and_inherit_the_root_dir() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./shared-root\nplugins_dir: [./plugins]\ninclude: [sub/mrsm-compose.yaml, own-root/mrsm-compose.yaml]\n",
        ),
        (
            "sub/mrsm-compose.yaml",
            "plugins_dir: [./sub-plugins]\npipes:\n  - connector: sql:local\n    metric: m\n    parameters:\n      note: $SUB_ONLY\n",
        ),
        ("sub/.env", "SUB_ONLY=from-sub-env\n"),
        (
            "own-root/mrsm-compose.yaml",
            "project_name: own\nroot_dir: ./mine\n",
        ),
    ]);
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // Its own name (the directory's), root inherited from the including project.
    let sub = project(&run, "sub");
    assert_eq!(sub.root_dir(), s.path("shared-root"));
    // A project that sets `root_dir` keeps it.
    assert_eq!(project(&run, "own").root_dir(), s.path("own-root/mine"));
    // Plugins: its own directory first, then the including project's (never a sibling's).
    let dirs: Vec<PathBuf> = sub.plugins_dirs();
    let own = dirs
        .iter()
        .position(|d| d.ends_with("sub/sub-plugins"))
        .unwrap();
    let top = dirs.iter().position(|d| *d == s.path("plugins"));
    assert!(own == 0 && top.is_some_and(|t| t > own), "{dirs:?}");
    assert!(
        !dirs.iter().any(|d| d.starts_with(s.path("own-root"))),
        "a sibling's plugins are never searched: {dirs:?}"
    );
    // `.env` is resolved from the project's own directory.
    let sub_pipe_note = sub
        .document
        .get("pipes")
        .and_then(Value::as_list)
        .and_then(|l| l.first())
        .and_then(|p| p.get("parameters"))
        .and_then(|p| p.get("note"))
        .map(meerschaum_core::pyfmt::py_str);
    assert_eq!(sub_pipe_note.as_deref(), Some("from-sub-env"));
}

// ---- overrides: the including project wins ---------------------------------------------------

fn override_tree() -> Sandbox {
    sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [sub.yaml]\nplugins: ['shared@api:custom']\nconfig:\n  meerschaum:\n    instance: sql:local\n    marker: root\n    connectors:\n      sql:\n        x:\n          flavor: sqlite\n          database: root.db\nenvironment:\n  SHARED: from-top\npipes:\n  - connector: sql:local\n    metric: both\n    parameters:\n      who: top\njobs:\n  mine: show version\n  sub/sync: show version --top\n",
        ),
        (
            "sub.yaml",
            "project_name: sub\nplugins: [shared, other]\nconfig:\n  meerschaum:\n    marker: sub\n    sub_only: kept\n    connectors:\n      sql:\n        x:\n          flavor: sqlite\n          database: sub.db\n          extra: kept\nenvironment:\n  SHARED: from-sub\n  SUB_ONLY: kept\npipes:\n  - connector: sql:local\n    metric: both\n    parameters:\n      who: sub\n  - connector: sql:local\n    metric: only-sub\njobs:\n  sync: show version --sub\n  other: show version --other\n",
        ),
    ])
}

#[test]
fn the_including_projects_values_win() {
    let s = override_tree();
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    let sub = project(&run, "sub");

    // `config` is deep-merged; the root's values win, the project's own keys stay, and the
    // project keeps its own name.
    let get = |keys: &[&str]| config_path(sub, keys).map(meerschaum_core::pyfmt::py_str);
    assert_eq!(
        get(&["config", "meerschaum", "marker"]).as_deref(),
        Some("root")
    );
    assert_eq!(
        get(&["config", "meerschaum", "sub_only"]).as_deref(),
        Some("kept")
    );
    assert_eq!(
        get(&["config", "meerschaum", "connectors", "sql", "x", "database"]).as_deref(),
        Some("root.db")
    );
    assert_eq!(
        get(&["config", "meerschaum", "connectors", "sql", "x", "extra"]).as_deref(),
        Some("kept")
    );
    assert_eq!(
        get(&["config", "stack", "project_name"]).as_deref(),
        Some("sub")
    );
    assert_eq!(sub.project_name(), "sub");

    // `environment:`: the root's variable wins.
    assert_eq!(get(&["environment", "SHARED"]).as_deref(), Some("from-top"));
    assert_eq!(get(&["environment", "SUB_ONLY"]).as_deref(), Some("kept"));

    // `plugins:` matched by name: the root's spec wins; the project's other plugin stays.
    assert_eq!(
        str_list(sub.config.get("plugins")),
        vec!["shared@api:custom".to_string(), "other".to_string()]
    );

    // `jobs:`: jobs stay per project; `<project>/<job>` in the root replaces one.
    let sub_jobs = sub.config.get("jobs").and_then(Value::as_map).unwrap();
    assert_eq!(
        sub_jobs
            .get("sync")
            .map(meerschaum_core::pyfmt::py_str)
            .as_deref(),
        Some("show version --top")
    );
    assert!(sub_jobs.contains_key("other"));
    let top_jobs = root_project(&run)
        .config
        .get("jobs")
        .and_then(Value::as_map)
        .unwrap();
    assert_eq!(top_jobs.keys().collect::<Vec<_>>(), vec!["mine"]);

    // Every override is an informational finding of `explain`.
    let info: Vec<&str> = run
        .out
        .lint
        .iter()
        .filter(|f| f.severity == Severity::Info)
        .map(|f| f.code)
        .collect();
    for code in [
        "override-config",
        "override-environment",
        "override-plugin",
        "override-job",
        "override-pipe",
    ] {
        assert!(info.contains(&code), "{code} missing from {info:?}");
    }
}

#[test]
fn a_pipe_the_root_defines_replaces_the_included_one() {
    let s = override_tree();
    let run = go_with(
        &s,
        "",
        &["up"],
        InMemoryInstance::default(),
        &mut InMemoryJobs::default(),
        &mut NoInput,
        |args| args.dry = true,
    );
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    let sub = project(&run, "sub");
    assert_eq!(sub.excluded_pipes.len(), 1);
    // The shared pipe is registered once, with the root's parameters and the root's tag; the
    // included project keeps its other pipe.
    let registered: Vec<&Vec<String>> = run
        .commands
        .iter()
        .filter(|c| c.starts_with(&["register".to_string(), "pipes".to_string()]))
        .collect();
    let metrics: Vec<&str> = registered
        .iter()
        .map(|c| c[c.iter().position(|a| a == "-m").unwrap() + 1].as_str())
        .collect();
    assert_eq!(metrics, vec!["only-sub", "both"], "{registered:?}");
    let both = registered[1];
    let params = &both[both.iter().position(|a| a == "--params").unwrap() + 1];
    assert!(
        params.contains("\"who\":\"top\"") && params.contains("\"top\""),
        "{params}"
    );
}

#[test]
fn up_runs_included_projects_before_the_root_and_down_reverses() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [a.yaml, b.yaml]\njobs:\n  tj: show version\n",
        ),
        ("a.yaml", "project_name: a\njobs:\n  aj: show version\n"),
        ("b.yaml", "project_name: b\njobs:\n  bj: show version\n"),
    ]);
    let run = go(&s, "", &["up"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    let started: Vec<String> = run
        .commands
        .iter()
        .filter(|c| c.contains(&"--name".to_string()))
        .map(|c| c[c.iter().position(|a| a == "--name").unwrap() + 1].clone())
        .collect();
    assert_eq!(started, vec!["aj", "bj", "tj"]);
    // Each runs under its own project name.
    let tags: Vec<String> = run
        .commands
        .iter()
        .filter(|c| c.contains(&"--name".to_string()))
        .map(|c| c[c.iter().position(|a| a == "-t").unwrap() + 1].clone())
        .collect();
    assert_eq!(tags, vec!["a", "b", "top"]);

    let mut jobs = InMemoryJobs::default();
    for (name, project) in [("aj", "a"), ("bj", "b"), ("tj", "top")] {
        jobs.jobs.insert(name.into(), owned_job(project, None));
    }
    let run = go_with(
        &s,
        "",
        &["down"],
        InMemoryInstance::default(),
        &mut jobs,
        &mut NoInput,
        |_| {},
    );
    let deleted: Vec<String> = run
        .commands
        .iter()
        .filter(|c| c.starts_with(&["delete".to_string(), "job".to_string()]))
        .map(|c| c[2].clone())
        .collect();
    assert_eq!(deleted, vec!["tj", "bj", "aj"], "down runs in reverse");
    assert_eq!(run.names(), vec!["top", "b", "a"]);
}

fn owned_job(project: &str, root: Option<&Path>) -> JobInfo {
    let mut env = IndexMap::new();
    env.insert("MRSM__COMPOSE_PROJECT".to_string(), project.to_string());
    if let Some(root) = root {
        env.insert(
            COMPOSE_ROOT_VAR.to_string(),
            root.to_string_lossy().to_string(),
        );
    }
    JobInfo {
        sysargs: vec!["show".into(), "version".into(), "-t".into(), project.into()],
        env,
        status: "running".into(),
        restart: false,
    }
}

#[test]
fn project_limits_an_operation_to_one_project() {
    let s = sandbox(&[
        ("mrsm-compose.yaml", ROOT_WITH_INCLUDES),
        (
            "projects/a.yaml",
            "project_name: a\njobs:\n  aj: show version\n",
        ),
        (
            "projects/b.yaml",
            "project_name: b\njobs:\n  bj: show version\n",
        ),
    ]);
    let run = go_with(
        &s,
        "",
        &["up"],
        InMemoryInstance::default(),
        &mut InMemoryJobs::default(),
        &mut NoInput,
        |args| args.project = Some("b".into()),
    );
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert_eq!(run.names(), vec!["b"]);
    assert!(run.commands.iter().any(|c| c.contains(&"bj".to_string())));
    assert!(!run.commands.iter().any(|c| c.contains(&"aj".to_string())));

    let run = go_with(
        &s,
        "",
        &["up"],
        InMemoryInstance::default(),
        &mut InMemoryJobs::default(),
        &mut NoInput,
        |args| args.project = Some("nope".into()),
    );
    let err = run.out.result.clone().unwrap_err();
    assert!(
        err.message.contains("No project named 'nope'"),
        "{}",
        err.message
    );
    assert!(err.message.contains("'top'") && err.message.contains("'a'"));
}

// ---- parent: -----------------------------------------------------------------------------------

fn parent_tree() -> Sandbox {
    sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [sub/mrsm-compose.yaml]\nenvironment:\n  FROM_TOP: top-value\nconfig:\n  meerschaum:\n    marker: top\n",
        ),
        (
            "sub/mrsm-compose.yaml",
            "project_name: sub\nparent: ..\njobs:\n  sj: show version\n",
        ),
    ])
}

#[test]
fn a_file_with_a_parent_runs_from_the_parents_tree_limited_to_itself() {
    let s = parent_tree();
    let run = go(&s, "sub", &["up"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // Only the subproject runs, with the parent's root, environment and configuration.
    assert_eq!(run.names(), vec!["sub"]);
    let sub = project(&run, "sub");
    assert_eq!(sub.root_dir(), s.path("root"));
    assert_eq!(
        config_path(sub, &["environment", "FROM_TOP"])
            .map(meerschaum_core::pyfmt::py_str)
            .as_deref(),
        Some("top-value")
    );
    assert_eq!(
        config_path(sub, &["config", "meerschaum", "marker"])
            .map(meerschaum_core::pyfmt::py_str)
            .as_deref(),
        Some("top")
    );
    assert_eq!(
        sub.compose_root.as_deref(),
        Some(s.path("mrsm-compose.yaml").as_path())
    );
    // `--standalone` ignores the parent.
    let run = go_with(
        &s,
        "sub",
        &["explain"],
        InMemoryInstance::default(),
        &mut InMemoryJobs::default(),
        &mut NoInput,
        |args| args.standalone = true,
    );
    let sub = project(&run, "sub");
    assert!(sub.compose_root.is_none());
    assert_eq!(sub.root_dir(), s.path("sub/root"));
}

#[test]
fn a_parent_that_does_not_include_the_project_treats_it_as_included() {
    let s = sandbox(&[
        ("mrsm-compose.yaml", "project_name: top\nroot_dir: ./root\n"),
        ("sub/mrsm-compose.yaml", "project_name: sub\nparent: ..\n"),
    ]);
    let run = go(&s, "sub", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert_eq!(run.names(), vec!["sub"]);
    assert!(
        run.warnings()
            .iter()
            .any(|w| w.contains("does not include it")),
        "{:?}",
        run.warnings()
    );
    assert_eq!(project(&run, "sub").root_dir(), s.path("root"));
}

#[test]
fn parents_chain_transitively_and_a_cycle_stops_at_the_last_valid_level() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [mid/mrsm-compose.yaml]\nenvironment:\n  LEVEL: top\n",
        ),
        (
            "mid/mrsm-compose.yaml",
            "project_name: mid\nparent: ..\ninclude: [leaf/mrsm-compose.yaml]\nenvironment:\n  LEVEL: mid\n  MID_ONLY: mid\n",
        ),
        (
            "mid/leaf/mrsm-compose.yaml",
            "project_name: leaf\nparent: ..\n",
        ),
    ]);
    let run = go(&s, "mid/leaf", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert_eq!(run.names(), vec!["leaf"]);
    let leaf = project(&run, "leaf");
    // The topmost wins per item; the middle project's own items pass down.
    let env = |k: &str| config_path(leaf, &["environment", k]).map(meerschaum_core::pyfmt::py_str);
    assert_eq!(env("LEVEL").as_deref(), Some("top"));
    assert_eq!(env("MID_ONLY").as_deref(), Some("mid"));
    assert_eq!(leaf.root_dir(), s.path("root"));

    // A cycle: a.yaml and b.yaml are each other's parent.
    let s = sandbox(&[
        ("a/mrsm-compose.yaml", "project_name: a\nparent: ../b\n"),
        (
            "b/mrsm-compose.yaml",
            "project_name: b\nparent: ../a\ninclude: ../a/mrsm-compose.yaml\n",
        ),
    ]);
    let run = go(&s, "a", &["explain"]);
    assert!(run.out.result.is_ok(), "{}", run.message());
    assert!(
        run.warnings().iter().any(|w| w.contains("`parent:` cycle")),
        "{:?}",
        run.warnings()
    );
}

// ---- the root label and the guard ------------------------------------------------------------

#[test]
fn jobs_started_from_a_root_carry_its_label_and_standalone_runs_leave_them_alone() {
    let s = sandbox(&[
        (
            "mrsm-compose.yaml",
            "project_name: top\nroot_dir: ./root\ninclude: [sub/mrsm-compose.yaml]\n",
        ),
        (
            "sub/mrsm-compose.yaml",
            "project_name: sub\njobs:\n  sj: show version\n",
        ),
    ]);
    let root_file = s.path("mrsm-compose.yaml");
    let run = go(&s, "", &["up"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // The started job's environment carries the label.
    let host = s.host("");
    let mut runner = RecordingRunner::default();
    let mut instance = InMemoryInstance::default();
    let mut jobs = InMemoryJobs::default();
    let args = ComposeArgs {
        action: vec!["up".into()],
        sysargs: vec!["compose".into(), "up".into()],
        yes: true,
        ..ComposeArgs::default()
    };
    let out = compose(
        &host,
        &mut Session::default(),
        &args,
        Services {
            runner: &mut runner,
            instance: &mut instance,
            jobs: &mut jobs,
            prompter: &mut NoInput,
        },
    );
    assert!(out.result.unwrap().success);
    let start = runner
        .commands
        .iter()
        .find(|c| c.sysargs.contains(&"sj".to_string()))
        .unwrap();
    let env = start.env.as_ref().or(start.inherited_env.as_ref()).unwrap();
    assert_eq!(
        env.get(COMPOSE_ROOT_VAR)
            .map(meerschaum_core::pyfmt::py_str),
        Some(meerschaum_compose::project::posix(&root_file))
    );

    // Running the subproject on its own (it has no `parent:`) leaves the root's job alone.
    let mut jobs = InMemoryJobs::default();
    jobs.jobs
        .insert("sj".into(), owned_job("sub", Some(&root_file)));
    let alone = go_with(
        &s,
        "sub",
        &["up"],
        InMemoryInstance::default(),
        &mut jobs,
        &mut NoInput,
        |_| {},
    );
    assert!(
        alone.out.result.as_ref().unwrap().success,
        "{}",
        alone.message()
    );
    assert!(
        alone
            .warnings()
            .iter()
            .any(|w| w.contains("managed by the root compose at")
                && w.contains("rerun from there or pass `--force`")),
        "{:?}",
        alone.warnings()
    );
    assert!(
        !alone.commands.iter().any(|c| c.contains(&"sj".to_string())),
        "the managed job is not restarted: {:?}",
        alone.commands
    );
    // `down` and a bare `stop jobs` skip it too.
    let down = go_with(
        &s,
        "sub",
        &["down"],
        InMemoryInstance::default(),
        &mut jobs,
        &mut NoInput,
        |_| {},
    );
    assert!(
        !down
            .commands
            .iter()
            .any(|c| c.starts_with(&["delete".to_string()])),
        "{:?}",
        down.commands
    );
    // `--force` takes over.
    let forced = go_with(
        &s,
        "sub",
        &["up"],
        InMemoryInstance::default(),
        &mut jobs,
        &mut NoInput,
        |args| args.force = true,
    );
    assert!(
        forced
            .commands
            .iter()
            .any(|c| c.contains(&"sj".to_string())),
        "{:?}",
        forced.commands
    );
}

// ---- missing references ----------------------------------------------------------------------

const MISSING: &str = "project_name: lint\nroot_dir: ./root\nplugins_dir: [./plugins]\nplugins: [listed-plugin]\nconnectors:\n  mqtt:\n    x: '{mqtt:nope}'\nconfig:\n  extra: 'MRSM{no:such:key}'\npipes:\n  - connector: plugin:absent\n    metric: a\n    instance: sql:local\n  - connector: sql:ghost\n    metric: b\n    instance: sql:local\n  - connector: sql:local\n    metric: c\n    instance: sql:phantom\n";

#[test]
fn explain_lists_every_missing_reference_and_keeps_going() {
    let s = sandbox(&[("mrsm-compose.yaml", MISSING)]);
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // The table is still printed.
    assert!(
        run.out
            .explain
            .as_deref()
            .unwrap_or_default()
            .contains("Pipes in 'lint'")
    );
    let find = |code: &str| -> Vec<String> {
        run.out
            .lint
            .iter()
            .filter(|f| f.code == code)
            .map(|f| f.message.clone())
            .collect()
    };
    let connectors = find("unresolved-connector");
    assert!(
        connectors.contains(
            &"connector 'mqtt:nope' is not defined on this host (referenced by config.meerschaum.connectors.mqtt.x)"
                .to_string()
        ),
        "{connectors:?}"
    );
    assert!(
        connectors
            .iter()
            .any(|m| m.contains("'sql:ghost'") && m.contains("pipes[1].connector")),
        "{connectors:?}"
    );
    assert!(
        connectors
            .iter()
            .any(|m| m.starts_with("instance 'sql:phantom'") && m.contains("pipes[2].instance")),
        "{connectors:?}"
    );
    let symlinks = find("unresolved-symlink");
    assert!(
        symlinks
            .iter()
            .any(|m| m.contains("MRSM{no:such:key}") && m.contains("config.extra")),
        "{symlinks:?}"
    );
    let missing_plugin = find("missing-plugin");
    assert!(
        missing_plugin
            .iter()
            .any(|m| m.contains("plugin 'absent'") && m.contains("pipes[0].connector")),
        "{missing_plugin:?}"
    );
    // What `up` would install is reported instead of installed.
    let pending: Vec<&str> = run
        .out
        .lint
        .iter()
        .filter(|f| f.code == "pending-plugin")
        .map(|f| f.message.as_str())
        .collect();
    assert!(
        pending
            .iter()
            .any(|m| m.contains("'listed-plugin'") && m.contains("would install")),
        "{pending:?}"
    );
}

#[test]
fn up_fails_on_a_missing_connector_naming_it_and_where_it_is_referenced() {
    let s = sandbox(&[(
        "mrsm-compose.yaml",
        "project_name: p\nroot_dir: ./root\nconnectors:\n  mqtt:\n    x: '{mqtt:nope}'\npipes:\n  - connector: sql:local\n    metric: m\n    instance: sql:local\n",
    )]);
    for action in ["up", "run"] {
        let run = go(&s, "", &[action]);
        let result = run.out.result.as_ref().unwrap();
        assert!(!result.success, "{action}");
        assert_eq!(
            result.message,
            "connector 'mqtt:nope' is not defined on this host (referenced by config.meerschaum.connectors.mqtt.x)"
        );
        assert!(!result.message.contains("TypeError"));
        // Nothing was registered or started.
        assert!(
            run.commands
                .iter()
                .all(|c| c[0] != "register" && c[0] != "sync"),
            "{:?}",
            run.commands
        );
    }
    // The passthrough `sync` refuses too; other actions still run.
    let run = go(&s, "", &["sync", "pipes"]);
    let result = run.out.result.as_ref().unwrap();
    assert!(!result.success);
    assert!(
        result.message.contains("connector 'mqtt:nope'"),
        "{}",
        result.message
    );
    let run = go(&s, "", &["show", "environment"]);
    assert!(run.out.result.as_ref().unwrap().success);
}

#[test]
fn explain_is_side_effect_free() {
    let s = sandbox(&[(
        "mrsm-compose.yaml",
        "project_name: fresh\nroot_dir: ./root\nplugins_dir: [./plugins]\nplugins: [needs-install]\npipes:\n  - connector: sql:local\n    metric: m\n    instance: sql:local\n",
    )]);
    let run = go(&s, "", &["explain"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    // No root directory, no config cache, no plugins directory, no command at all.
    assert!(!s.path("root").exists());
    assert!(!s.path("plugins").exists());
    assert!(run.commands.is_empty(), "{:?}", run.commands);
    assert!(
        !run.out.messages.items.iter().any(|m| m
            .to_string()
            .contains("Initializing Meerschaum root directory")),
        "{:?}",
        run.out.messages
    );
    // Every file of the project is untouched.
    let mut files: Vec<String> = std::fs::read_dir(&s.base)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    files.sort();
    assert_eq!(
        files,
        ["home", "host-plugins", "host-root", "mrsm-compose.yaml"].map(String::from)
    );
}

// ---- compose init adds the pipes the filters match -------------------------------------------

#[test]
fn init_with_filters_adds_the_matching_registered_pipes() {
    let s = sandbox(&[]);
    std::fs::create_dir_all(s.path("newproject")).unwrap();
    let mut instance = InMemoryInstance::default();
    let registered = |id: i64, ck: &str, mk: &str, lk: Option<&str>, tag: &str| RegisteredPipe {
        connector_keys: ck.into(),
        metric_key: mk.into(),
        location_key: lk.map(String::from),
        instance_keys: "sql:local".into(),
        id,
        parameters: meerschaum_core::value::Map::from_iter([(
            "tags".to_string(),
            Value::List(vec![Value::Str(tag.into())]),
        )]),
    };
    instance.pipes = vec![
        registered(1, "sql:a", "temp", Some("atlanta"), "keep"),
        registered(2, "sql:a", "rain", None, "keep"),
        registered(3, "sql:b", "temp", None, "other"),
    ];
    let mut prompter = ScriptedPrompter {
        answers: vec![Some(true); 4],
        questions: Vec::new(),
    };
    let run = go_with(
        &s,
        "newproject",
        &["init"],
        instance,
        &mut InMemoryJobs::default(),
        &mut prompter,
        |args| {
            args.connector_keys = vec!["sql:a".into()];
            args.metric_keys = vec!["temp".into()];
            args.yes = false;
        },
    );
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert_eq!(prompter.questions.len(), 3, "{:?}", prompter.questions);
    assert!(prompter.questions[1].starts_with("Add pipes from the given filters"));
    assert!(prompter.questions[2].starts_with("Add these pipes"));
    let (_, text) = run
        .out
        .written_files
        .first()
        .expect("a compose file was written");
    assert!(
        text.contains("metric: temp") && text.contains("location: atlanta"),
        "{text}"
    );
    assert!(!text.contains("rain") && !text.contains("sql:b"), "{text}");
    // The same file with the pipes declined.
    let _ = run.instance;
    let s = sandbox(&[]);
    std::fs::create_dir_all(s.path("p2")).unwrap();
    let mut prompter = ScriptedPrompter {
        answers: vec![Some(true), Some(true), Some(false)],
        questions: Vec::new(),
    };
    let inst = InMemoryInstance {
        pipes: vec![registered(1, "sql:a", "temp", None, "keep")],
        ..InMemoryInstance::default()
    };
    let run = go_with(
        &s,
        "p2",
        &["init"],
        inst,
        &mut InMemoryJobs::default(),
        &mut prompter,
        |args| {
            args.tags = vec!["keep".into()];
            args.yes = false;
        },
    );
    let (_, text) = run.out.written_files.first().unwrap();
    assert!(!text.contains("metric: temp"), "{text}");
    assert!(
        run.out
            .messages
            .items
            .iter()
            .any(|m| m.to_string().contains("No pipes will be added"))
    );
}

// ---- regressions: job commands the corpora use -----------------------------------------------

#[test]
fn per_file_start_and_stop_by_name_in_a_shared_root() {
    // Two compose files share one root (`--file` per file, the v4 multi-file pattern).
    let s = sandbox(&[
        (
            "a.yaml",
            "project_name: a\nroot_dir: ./root\njobs:\n  ja: show version\n",
        ),
        (
            "b.yaml",
            "project_name: b\nroot_dir: ./root\njobs:\n  jb: show version\n",
        ),
    ]);
    let mut jobs = InMemoryJobs::default();
    let mut ja = owned_job("a", None);
    ja.status = "stopped".into();
    let mut jb = owned_job("b", None);
    jb.status = "stopped".into();
    jobs.jobs.insert("ja".into(), ja);
    jobs.jobs.insert("jb".into(), jb);
    let act = |verb: &str, name: &str, file: &str, jobs: &mut InMemoryJobs| {
        go_with(
            &s,
            "",
            &[verb, "jobs", name],
            InMemoryInstance::default(),
            jobs,
            &mut NoInput,
            |args| args.file = Some(PathBuf::from(file)),
        )
    };
    let ok = act("start", "ja", "a.yaml", &mut jobs);
    assert!(ok.out.result.as_ref().unwrap().success, "{}", ok.message());
    assert!(
        ok.commands
            .iter()
            .any(|c| c[..3] == ["start", "jobs", "ja"]),
        "{:?}",
        ok.commands
    );
    // A job of the other file is refused from this one, and works from its own.
    let refused = act("stop", "jb", "a.yaml", &mut jobs);
    assert!(!refused.out.result.as_ref().unwrap().success);
    assert!(
        refused
            .message()
            .contains("owned by another Compose project")
    );
    assert!(
        !refused.commands.iter().any(|c| c[0] == "stop"),
        "{:?}",
        refused.commands
    );
    let ok = act("start", "jb", "b.yaml", &mut jobs);
    assert!(ok.out.result.as_ref().unwrap().success, "{}", ok.message());
    assert!(
        ok.commands
            .iter()
            .any(|c| c[..3] == ["start", "jobs", "jb"])
    );
}

#[test]
fn listen_and_chained_jobs_are_started_and_adopted() {
    let s = sandbox(&[(
        "mrsm-compose.yaml",
        "project_name: chain\nroot_dir: ./root\njobs:\n  listener: sync pipes -c plugin:feed --listen\n  chained: >\n    sync pipes -c plugin:a -t chain\n    + sync pipes -c sql:local -m b -t chain\n    : --loop --min-seconds 60 -w 1\n",
    )]);
    let run = go(&s, "", &["up"]);
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    let start = |name: &str| -> Vec<String> {
        run.commands
            .iter()
            .find(|c| c.windows(2).any(|w| w == ["--name", name]))
            .unwrap_or_else(|| panic!("no start of {name} in {:?}", run.commands))
            .clone()
    };
    let listener = start("listener");
    assert!(listener.contains(&"--listen".to_string()) && listener.contains(&"-d".to_string()));
    let chained = start("chained");
    for part in ["+", ":", "--loop", "--min-seconds", "-w"] {
        assert!(chained.contains(&part.to_string()), "{part} in {chained:?}");
    }
    assert!(chained.contains(&"-d".to_string()) && chained.contains(&"-f".to_string()));
    // The running jobs are recognized as the project's on the next `up` (and replaced, not
    // refused), including the chain recorded as a compressed pipeline.
    let mut jobs = InMemoryJobs::default();
    let mut running = |name: &str, command: Vec<String>| {
        let mut sysargs: Vec<String> = command.into_iter().filter(|a| a != "-d").collect();
        sysargs.retain(|a| !a.is_empty());
        let mut env = IndexMap::new();
        env.insert("MRSM__COMPOSE_PROJECT".to_string(), "chain".to_string());
        jobs.jobs.insert(
            name.to_string(),
            JobInfo {
                sysargs,
                env,
                status: "running".into(),
                restart: false,
            },
        );
    };
    running("listener", listener);
    running("chained", chained);
    let again = go_with(
        &s,
        "",
        &["up"],
        InMemoryInstance::default(),
        &mut jobs,
        &mut NoInput,
        |_| {},
    );
    assert!(
        again.out.result.as_ref().unwrap().success,
        "{}",
        again.message()
    );
    let deleted: Vec<&Vec<String>> = again
        .commands
        .iter()
        .filter(|c| c.starts_with(&["delete".to_string(), "job".to_string()]))
        .collect();
    assert_eq!(deleted.len(), 2, "{:?}", again.commands);
}

#[test]
fn the_registered_event_log_of_a_tree_lists_no_edits_for_dry_runs_of_unchanged_pipes() {
    // A dry `up` over a tree with an already-registered pipe edits nothing.
    let s = sandbox(&[
        ("mrsm-compose.yaml", ROOT_WITH_INCLUDES),
        (
            "projects/a.yaml",
            "project_name: a\npipes:\n  - connector: sql:local\n    metric: m\n    instance: sql:local\n",
        ),
    ]);
    let mut instance = InMemoryInstance::default();
    instance.pipes.push(RegisteredPipe {
        connector_keys: "sql:local".into(),
        metric_key: "m".into(),
        location_key: None,
        instance_keys: "sql:local".into(),
        id: 7,
        parameters: meerschaum_core::value::Map::from_iter([(
            "tags".to_string(),
            Value::List(vec![Value::Str("a".into())]),
        )]),
    });
    let run = go_with(
        &s,
        "",
        &["up"],
        instance,
        &mut InMemoryJobs::default(),
        &mut NoInput,
        |args| args.dry = true,
    );
    assert!(
        run.out.result.as_ref().unwrap().success,
        "{}",
        run.message()
    );
    assert!(
        !run.instance
            .events
            .iter()
            .any(|e| matches!(e, InstanceEvent::Edit(..))),
        "{:?}",
        run.instance.events
    );
}

#[test]
fn job_starts_are_bulk_starts_for_the_startup_gate() {
    let s = sandbox(&[(
        "mrsm-compose.yaml",
        "project_name: one\nroot_dir: ./root\njobs:\n  only: show version\n",
    )]);
    let kind = |tweak: fn(&mut ComposeArgs)| -> Option<String> {
        let host = s.host("");
        let mut runner = RecordingRunner::default();
        let mut args = ComposeArgs {
            action: vec!["up".into()],
            sysargs: vec!["compose".into(), "up".into()],
            yes: true,
            ..ComposeArgs::default()
        };
        tweak(&mut args);
        let out = compose(
            &host,
            &mut Session::default(),
            &args,
            Services {
                runner: &mut runner,
                instance: &mut InMemoryInstance::default(),
                jobs: &mut InMemoryJobs::default(),
                prompter: &mut NoInput,
            },
        );
        assert!(out.result.unwrap().success);
        let start = runner
            .commands
            .iter()
            .find(|c| c.sysargs.contains(&"only".to_string()))
            .unwrap();
        start
            .env
            .as_ref()
            .or(start.inherited_env.as_ref())
            .and_then(|e| e.get("MRSM_JOB_START_KIND"))
            .map(meerschaum_core::pyfmt::py_str)
    };
    // Even one job: it is the automatic start of a project, not an explicit `start job`.
    assert_eq!(kind(|_| {}).as_deref(), Some("bulk"));
    assert_eq!(kind(|a| a.no_stagger = true).as_deref(), Some("none"));
}
