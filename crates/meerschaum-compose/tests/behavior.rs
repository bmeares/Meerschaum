//! Behavior tests beyond the Python fixtures: the lint, the typed model over every fixture
//! project, the one-time plugin warning, and prompting.

use std::path::{Path, PathBuf};

use meerschaum_compose::command::RecordingRunner;
use meerschaum_compose::model::{ComposeFile, ConnectorSpec};
use meerschaum_compose::ops::{InMemoryInstance, InMemoryJobs, JobInfo, NoInput, ScriptedPrompter};
use meerschaum_compose::project::{Host, Session};
use meerschaum_compose::subactions::{ComposeArgs, ComposeOutput, Services, compose};

fn projects() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/projects")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &dst.join(entry.file_name()));
        } else {
            std::fs::copy(&path, dst.join(entry.file_name())).unwrap();
        }
    }
}

struct Env {
    _tmp: tempfile::TempDir,
    base: PathBuf,
    project: PathBuf,
}

fn env_for(name: &str) -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let base = std::fs::canonicalize(tmp.path()).unwrap();
    for sub in ["host-root", "host-plugins", "home"] {
        std::fs::create_dir_all(base.join(sub)).unwrap();
    }
    let project = base.join(name);
    copy_dir(&projects().join(name), &project);
    Env {
        _tmp: tmp,
        base,
        project,
    }
}

fn host(env: &Env) -> Host {
    let mut vars = indexmap::IndexMap::new();
    let p = |s: &str| env.base.join(s).to_string_lossy().to_string();
    vars.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
    vars.insert("HOME".to_string(), p("home"));
    vars.insert("MRSM_ROOT_DIR".to_string(), p("host-root"));
    vars.insert("MRSM_PLUGINS_DIR".to_string(), p("host-plugins"));
    Host::new(env.project.clone(), vars).unwrap()
}

#[test]
fn project_environment_layers_keep_file_precedence_and_symlink_targets() {
    use meerschaum_compose::error::Messages;
    use meerschaum_compose::pipes::ProjectConfig;
    use meerschaum_compose::project::load_project;
    use meerschaum_core::Value;

    let env = env_for("basic");
    std::fs::create_dir_all(env.project.join("root")).unwrap();
    std::fs::write(env.project.join("mrsm-compose.yaml"), r#"
project_name: layers
root_dir: ./root
plugins_dir: [./plugins]
config:
  custom:
    host: file-host
  meerschaum:
    connectors:
      sql:
        example:
          host: 'MRSM{custom:host}'
          database: file.db
environment:
  MRSM_SQL_EXAMPLE: '{"host":"env-host","port":5559}'
  MRSM_API_OTHER: '{invalid}'
  MRSM_CONFIG: '{"custom":{"host":"ignored","config_only":true}}'
  MRSM_PATCH: '{"custom":{"patched":true},"meerschaum":{"connectors":{"sql":{"example":{"database":"patch.db","username":"patch-user"}}}}}'
"#).unwrap();
    let host = host(&env);
    let project = load_project(&host, None, None, false, &mut Messages::default()).unwrap();
    let config = ProjectConfig::new(&host, &project).unwrap();
    let get = |key| config.get(&["meerschaum", "connectors", "sql", "example", key]);
    assert_eq!(get("host"), Some(Value::from("file-host")));
    assert_eq!(get("database"), Some(Value::from("file.db")));
    assert_eq!(get("port"), Some(Value::Int(5559)));
    assert_eq!(get("username"), Some(Value::from("patch-user")));
    assert_eq!(
        config.get(&["custom", "host"]),
        Some(Value::from("file-host"))
    );
    assert_eq!(config.get(&["custom", "patched"]), Some(Value::Bool(true)));
    assert_eq!(config.get(&["custom", "config_only"]), None);
    assert_eq!(
        config.get(&["meerschaum", "connectors", "api", "other", "uri"]),
        Some(Value::from("{invalid}"))
    );
    assert!(!project.root_dir().join("config").exists());

    // With no URI connectors to build, MRSM_PATCH directly patches the project block.
    let mut patch_only = project.clone();
    let environment = patch_only
        .config
        .get_mut("environment")
        .unwrap()
        .as_map_mut()
        .unwrap();
    environment.shift_remove("MRSM_SQL_EXAMPLE");
    environment.shift_remove("MRSM_API_OTHER");
    let patch_only_config = ProjectConfig::new(&host, &patch_only).unwrap();
    assert_eq!(
        patch_only_config.get(&["meerschaum", "connectors", "sql", "example", "database"]),
        Some(Value::from("patch.db"))
    );

    // A bad patch is ignored without preventing connector application or file restoration.
    let mut project = project;
    project
        .config
        .get_mut("environment")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("MRSM_PATCH".into(), Value::from("{"));
    let config = ProjectConfig::new(&host, &project).unwrap();
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "example", "database"]),
        Some(Value::from("file.db"))
    );
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "example", "port"]),
        Some(Value::Int(5559))
    );
}

fn run(
    host: &Host,
    session: &mut Session,
    action: &[&str],
    jobs: &mut dyn meerschaum_compose::ops::JobOps,
    prompter: &mut dyn meerschaum_compose::ops::Prompter,
    yes: bool,
) -> ComposeOutput {
    let mut sysargs = vec!["compose".to_string()];
    sysargs.extend(action.iter().map(|s| s.to_string()));
    let args = ComposeArgs {
        action: action.iter().map(|s| s.to_string()).collect(),
        sysargs,
        yes,
        width: Some(100),
        ..ComposeArgs::default()
    };
    let mut runner = RecordingRunner::default();
    let mut instance = InMemoryInstance::default();
    compose(
        host,
        session,
        &args,
        Services {
            runner: &mut runner,
            instance: &mut instance,
            jobs,
            prompter,
        },
    )
}

fn lint_codes(name: &str) -> (Vec<(String, String)>, ComposeOutput) {
    let env = env_for(name);
    let host = host(&env);
    let out = run(
        &host,
        &mut Session::default(),
        &["explain"],
        &mut InMemoryJobs::default(),
        &mut NoInput,
        false,
    );
    let codes = out
        .lint
        .iter()
        .map(|w| (w.code.to_string(), w.location.clone()))
        .collect();
    (codes, out)
}

#[test]
fn lint_reports_without_changing_behavior() {
    let (codes, out) = lint_codes("lint");
    let has = |code: &str, loc: &str| codes.iter().any(|(c, l)| c == code && l.contains(loc));
    assert!(has("unknown-key", "piped"), "{codes:?}");
    assert!(has("missing-plugins-dir", "plugins_dir[1]"), "{codes:?}");
    assert!(has("misspelled-key", "parameters.dtype"), "{codes:?}");
    assert!(has("misspelled-key", "parameters.tag"), "{codes:?}");
    assert!(has("misspelled-key", "parameters.index"), "{codes:?}");
    assert!(has("string-tags", "parameters.tags"), "{codes:?}");
    assert!(has("unresolved-reference", "parameters.sql"), "{codes:?}");
    // `indexes:` is an accepted alias, and the defined `source` pipe is not reported.
    assert!(!has("misspelled-key", "indexes"), "{codes:?}");
    let unresolved: Vec<_> = out
        .lint
        .iter()
        .filter(|w| w.code == "unresolved-reference")
        .collect();
    assert_eq!(unresolved.len(), 1, "{unresolved:?}");
    assert!(unresolved[0].message.contains("'stale'"));
    // The lint never changes the outcome.
    assert!(out.result.as_ref().unwrap().success);

    let (codes, _) = lint_codes("nested-pipes");
    assert!(codes.iter().any(|(c, _)| c == "nested-pipes"), "{codes:?}");

    let (codes, out) = lint_codes("kwarg-typo");
    assert!(
        codes
            .iter()
            .any(|(c, l)| c == "misspelled-key" && l.ends_with(".dtype")),
        "{codes:?}"
    );
    assert!(out.result.is_err());

    let (codes, out) = lint_codes("string-tags");
    assert!(
        codes
            .iter()
            .any(|(c, l)| c == "string-tags" && l.ends_with(".tags")),
        "{codes:?}"
    );
    assert!(out.result.is_err());

    // Nothing to warn about; `up` would install the listed plugin (an informational finding).
    let (codes, _) = lint_codes("basic");
    assert!(
        codes.iter().all(|(c, _)| c == "pending-plugin"),
        "{codes:?}"
    );
}

#[test]
fn model_parses_every_fixture_project() {
    for entry in std::fs::read_dir(projects()).unwrap() {
        let dir = entry.unwrap().path();
        for file in std::fs::read_dir(&dir).unwrap() {
            let file = file.unwrap().path();
            let name = file.file_name().unwrap().to_string_lossy().to_string();
            if !(name.starts_with("mrsm-compose")
                && (name.ends_with(".yaml") || name.ends_with(".yml")))
            {
                continue;
            }
            let text = std::fs::read_to_string(&file).unwrap();
            let doc = meerschaum_core::yaml::load(&text).unwrap();
            let model =
                ComposeFile::from_value(&doc).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
            let back = model.to_value().unwrap();
            assert_eq!(
                back.as_map().map(|m| m.len()),
                doc.as_map().map(|m| m.len()),
                "{}",
                file.display()
            );
        }
    }
    let text = std::fs::read_to_string(projects().join("host-refs/mrsm-compose.yaml")).unwrap();
    let model = meerschaum_compose::model::parse_yaml(&text).unwrap();
    let sql = &model.connectors.as_ref().unwrap()["sql"];
    assert!(
        matches!(&sql["alias"], ConnectorSpec::HostReference { typ, label } if typ == "sql" && label == "local")
    );
    assert!(matches!(&sql["invalid"], ConnectorSpec::Invalid(_)));
    assert!(matches!(&sql["too_many"], ConnectorSpec::Invalid(_)));
    assert!(matches!(&sql["empty"], ConnectorSpec::Null));
}

#[test]
fn compose_plugin_warning_is_once_per_session() {
    let env = env_for("compose-plugin");
    std::fs::write(env.base.join("host-plugins/compose.py"), "").unwrap();
    let host = host(&env);
    let mut session = Session::default();
    let count = |out: &ComposeOutput| {
        out.messages
            .warnings()
            .iter()
            .filter(|w| w.contains("mrsm uninstall plugin compose"))
            .count()
    };
    let mut jobs = InMemoryJobs::default();
    let first = run(&host, &mut session, &["ps"], &mut jobs, &mut NoInput, false);
    let second = run(&host, &mut session, &["ps"], &mut jobs, &mut NoInput, false);
    assert_eq!(count(&first), 1);
    assert_eq!(count(&second), 0);
}

#[test]
fn bare_stop_jobs_asks_for_confirmation() {
    let env = env_for("registered");
    let host = host(&env);
    let mut jobs = InMemoryJobs::default();
    jobs.jobs.insert(
        "api".into(),
        JobInfo {
            sysargs: vec!["start".into(), "api".into(), "-t".into(), "reg".into()],
            status: "running".into(),
            ..JobInfo::default()
        },
    );
    let mut prompter = ScriptedPrompter {
        answers: vec![Some(false)],
        ..ScriptedPrompter::default()
    };
    let out = run(
        &host,
        &mut Session::default(),
        &["stop", "jobs"],
        &mut jobs,
        &mut prompter,
        false,
    );
    let result = out.result.unwrap();
    assert!(!result.success);
    assert_eq!(result.message, "No jobs were stopped.");
    assert_eq!(prompter.questions, vec!["Stop the above jobs?".to_string()]);
    assert_eq!(out.commands.len(), 1, "only `show version` ran");
}

/// An executor whose reads fail (an unreachable API, a broken `systemctl`).
struct FailingJobs;

impl meerschaum_compose::ops::JobOps for FailingJobs {
    fn get_jobs(&mut self) -> indexmap::IndexMap<String, JobInfo> {
        indexmap::IndexMap::new()
    }

    fn try_get_jobs(&mut self) -> Result<indexmap::IndexMap<String, JobInfo>, String> {
        Err("connection refused".into())
    }

    fn take_warnings(&mut self) -> Vec<String> {
        vec!["Job 'x' exists in both `local` and `systemd`.".into()]
    }
}

#[test]
fn a_failed_executor_read_stops_down_instead_of_reading_as_no_jobs() {
    let env = env_for("registered");
    let host = host(&env);
    let out = run(
        &host,
        &mut Session::default(),
        &["down"],
        &mut FailingJobs,
        &mut NoInput,
        true,
    );
    let result = out.result.unwrap();
    assert!(!result.success, "{result:?}");
    assert!(
        result
            .message
            .contains("Could not read the jobs of this project's executor")
            && result.message.contains("connection refused"),
        "{}",
        result.message
    );
    assert!(
        out.messages
            .warnings()
            .iter()
            .any(|w| w.contains("exists in both `local` and `systemd`")),
        "listing warnings are surfaced"
    );
    assert!(
        out.commands
            .iter()
            .all(|c| !c.sysargs.iter().any(|a| a == "delete")),
        "nothing was deleted: {:?}",
        out.commands
    );
}

// ---- Compose job safety (ported from the v4 `tests/test_compose.py`): ownership, scoping and
// failure propagation, over a fake jobs supervisor, instance and command runner.

/// A project directory written from `yaml` (the project is named `alpha`).
fn env_from_yaml(yaml: &str) -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let base = std::fs::canonicalize(tmp.path()).unwrap();
    for sub in ["host-root", "host-plugins", "home"] {
        std::fs::create_dir_all(base.join(sub)).unwrap();
    }
    let project = base.join("alpha");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("mrsm-compose.yaml"), yaml).unwrap();
    Env {
        _tmp: tmp,
        base,
        project,
    }
}

const ALPHA: &str =
    "project_name: alpha\nroot_dir: ./root\njobs:\n  api: start api\n  worker: sync pipes --loop\n";

struct Outcome {
    out: ComposeOutput,
    /// The sysargs of every command compose ran.
    commands: Vec<Vec<String>>,
}

#[allow(clippy::too_many_arguments)]
fn run_with(
    env: &Env,
    action: &[&str],
    jobs: &mut dyn meerschaum_compose::ops::JobOps,
    instance: &mut InMemoryInstance,
    runner: &mut RecordingRunner,
    prompter: &mut dyn meerschaum_compose::ops::Prompter,
    yes: bool,
    drop: bool,
    tweak: impl FnOnce(&mut ComposeArgs),
) -> ComposeOutput {
    let host = host(env);
    let mut sysargs = vec!["compose".to_string()];
    sysargs.extend(action.iter().map(|s| s.to_string()));
    let mut args = ComposeArgs {
        action: action.iter().map(|s| s.to_string()).collect(),
        sysargs,
        yes,
        drop,
        width: Some(100),
        ..ComposeArgs::default()
    };
    tweak(&mut args);
    compose(
        &host,
        &mut Session::default(),
        &args,
        Services {
            runner,
            instance,
            jobs,
            prompter,
        },
    )
}

fn scoped(env: &Env, action: &[&str], jobs: &mut InMemoryJobs, yes: bool) -> Outcome {
    let mut runner = RecordingRunner::default();
    let out = run_with(
        env,
        action,
        jobs,
        &mut InMemoryInstance::default(),
        &mut runner,
        &mut NoInput,
        yes,
        false,
        |_| {},
    );
    Outcome {
        out,
        commands: runner.commands.into_iter().map(|c| c.sysargs).collect(),
    }
}

fn job(sysargs: &[&str], project: Option<&str>, status: &str, restart: bool) -> JobInfo {
    let mut env = indexmap::IndexMap::new();
    if let Some(project) = project {
        env.insert("MRSM__COMPOSE_PROJECT".to_string(), project.to_string());
    }
    JobInfo {
        sysargs: sysargs.iter().map(|s| s.to_string()).collect(),
        env,
        status: status.into(),
        restart,
    }
}

fn jobs_of(entries: Vec<(&str, JobInfo)>) -> InMemoryJobs {
    InMemoryJobs {
        jobs: entries
            .into_iter()
            .map(|(n, j)| (n.to_string(), j))
            .collect(),
    }
}

/// The commands which are not the `show version` the project init runs first.
fn acting(commands: &[Vec<String>]) -> Vec<Vec<String>> {
    commands
        .iter()
        .filter(|c| {
            !(c.len() >= 2
                && c[0] == "show"
                && c[1] == "version"
                && !c.iter().any(|a| a == "--name"))
        })
        .cloned()
        .collect()
}

fn message(outcome: &Outcome) -> String {
    outcome.out.result.as_ref().unwrap().message.clone()
}

fn succeeded(outcome: &Outcome) -> bool {
    outcome.out.result.as_ref().unwrap().success
}

#[test]
fn job_actions_without_names_are_scoped_to_the_project() {
    let env = env_from_yaml(ALPHA);
    let mut jobs = jobs_of(vec![
        (
            "api",
            job(&["start", "api"], Some("alpha"), "running", true),
        ),
        (
            "worker",
            job(
                &[
                    "sync", "pipes", "--loop", "-t", "alpha", "--name", "worker", "-f",
                ],
                None,
                "stopped",
                false,
            ),
        ),
        (
            "other",
            job(
                &["sync", "pipes", "-c", "acme:one"],
                Some("beta"),
                "stopped",
                false,
            ),
        ),
        ("manual", job(&["show", "version"], None, "stopped", false)),
    ]);
    // Each bare command expands to the project jobs it would have acted on: `start` never
    // restarts the running `api`, and `restart` only checks `--restart` jobs.
    let expected: [(&str, &[&str]); 5] = [
        ("start", &["worker"]),
        ("stop", &["api"]),
        ("pause", &["api"]),
        ("restart", &["api"]),
        ("delete", &["api", "worker"]),
    ];
    for (verb, names) in expected {
        let o = scoped(&env, &[verb, "jobs"], &mut jobs, true);
        assert!(succeeded(&o), "{verb}: {}", message(&o));
        let mut command = vec![verb.to_string(), "jobs".to_string()];
        command.extend(names.iter().map(|n| n.to_string()));
        command.extend(["--tags".to_string(), "alpha".to_string()]);
        assert_eq!(acting(&o.commands), vec![command], "{verb}");
    }
}

#[test]
fn start_jobs_leaves_running_jobs_alone() {
    let env = env_from_yaml(ALPHA);
    let mut jobs = jobs_of(vec![
        (
            "api",
            job(&["start", "api"], Some("alpha"), "running", false),
        ),
        (
            "worker",
            job(
                &[
                    "sync", "pipes", "--loop", "-t", "alpha", "--name", "worker", "-f",
                ],
                None,
                "running",
                false,
            ),
        ),
    ]);
    let o = scoped(&env, &["start", "jobs"], &mut jobs, true);
    assert!(succeeded(&o));
    assert_eq!(message(&o), "All jobs are running.");
    assert!(acting(&o.commands).is_empty(), "{:?}", o.commands);
}

#[test]
fn job_actions_refuse_other_projects_jobs() {
    let env = env_from_yaml(ALPHA);
    let mut jobs = jobs_of(vec![
        (
            "api",
            job(&["start", "api"], Some("alpha"), "stopped", false),
        ),
        (
            "other",
            job(
                &["sync", "pipes", "-c", "acme:one"],
                Some("beta"),
                "stopped",
                false,
            ),
        ),
        ("manual", job(&["show", "version"], None, "stopped", false)),
    ]);
    let o = scoped(&env, &["start", "jobs", "api", "other"], &mut jobs, true);
    assert!(!succeeded(&o));
    let msg = message(&o);
    assert!(
        msg.contains("other") && !msg.rsplit(':').next().unwrap().contains("api"),
        "{msg}"
    );
    assert!(acting(&o.commands).is_empty(), "{:?}", o.commands);

    // This project's jobs and unstamped jobs may still be named explicitly.
    let o = scoped(&env, &["start", "job", "api", "manual"], &mut jobs, true);
    assert!(succeeded(&o), "{}", message(&o));
    assert_eq!(
        acting(&o.commands),
        vec![vec!["start", "job", "api", "manual", "--tags", "alpha"]]
    );
}

#[test]
fn job_actions_with_no_project_jobs_do_nothing() {
    let env = env_from_yaml(ALPHA);
    let mut jobs = jobs_of(vec![(
        "other",
        job(
            &["sync", "pipes", "-c", "acme:one"],
            Some("beta"),
            "stopped",
            false,
        ),
    )]);
    let o = scoped(&env, &["stop", "jobs"], &mut jobs, true);
    assert!(succeeded(&o));
    assert!(message(&o).contains("No jobs"), "{}", message(&o));
    assert!(acting(&o.commands).is_empty(), "{:?}", o.commands);
}

#[test]
fn other_actions_pass_through_unscoped() {
    let env = env_from_yaml(ALPHA);
    let mut jobs = jobs_of(vec![(
        "other",
        job(&["sync", "pipes"], Some("beta"), "stopped", false),
    )]);
    for action in [["show", "jobs"], ["show", "pipes"], ["sync", "pipes"]] {
        let o = scoped(&env, &action, &mut jobs, true);
        assert!(succeeded(&o), "{action:?}: {}", message(&o));
        let ran = acting(&o.commands);
        assert_eq!(ran.len(), 1, "{action:?}: {ran:?}");
        assert!(ran[0].starts_with(&[action[0].to_string(), action[1].to_string()]));
        // (no job names were inserted)
        assert!(!ran[0].iter().any(|a| a == "other"), "{ran:?}");
    }
}

#[test]
fn down_deletes_only_exact_owned_job_names() {
    let env = env_from_yaml("project_name: alpha\nroot_dir: ./root\njobs:\n  api: start api\n");
    let mut jobs = jobs_of(vec![
        (
            "api",
            job(&["start", "api", "-t", "alpha"], None, "running", false),
        ),
        (
            "foreign",
            job(&["start", "api", "-t", "beta"], None, "running", false),
        ),
    ]);
    let o = scoped(&env, &["down"], &mut jobs, true);
    assert!(succeeded(&o), "{}", message(&o));
    let deletes: Vec<_> = o
        .commands
        .iter()
        .filter(|c| c.first().map(String::as_str) == Some("delete"))
        .collect();
    assert_eq!(deletes.len(), 1, "{:?}", o.commands);
    assert_eq!(deletes[0][..4], ["delete", "job", "api", "-f"]);
}

#[test]
fn down_propagates_delete_failures() {
    let env = env_from_yaml("project_name: alpha\nroot_dir: ./root\njobs:\n  api: start api\n");
    let mut jobs = jobs_of(vec![(
        "api",
        job(&["start", "api", "-t", "alpha"], None, "running", false),
    )]);
    let mut runner = RecordingRunner::default();
    runner.results.push((
        vec!["delete".to_string()],
        meerschaum_core::SuccessTuple::fail("delete failed"),
    ));
    let out = run_with(
        &env,
        &["down"],
        &mut jobs,
        &mut InMemoryInstance::default(),
        &mut runner,
        &mut NoInput,
        true,
        false,
        |_| {},
    );
    let result = out.result.unwrap();
    assert!(!result.success);
    assert!(
        result.message.contains("delete failed"),
        "{}",
        result.message
    );
}

#[test]
fn down_untags_shared_pipes_and_deletes_only_unshared() {
    use meerschaum_compose::ops::{InstanceEvent, RegisteredPipe};
    use meerschaum_core::value::{Map, Value};
    let env = env_from_yaml(
        "project_name: alpha\nroot_dir: ./root\npipes:\n  - connector: plugin:foo\n    metric: shared\n    instance: sql:local\n  - connector: plugin:foo\n    metric: owned\n    instance: sql:local\n",
    );
    let registered = |metric: &str, id: i64, tags: &[&str]| {
        let mut parameters = Map::new();
        parameters.insert(
            "tags".into(),
            Value::List(tags.iter().map(|t| Value::Str(t.to_string())).collect()),
        );
        RegisteredPipe {
            connector_keys: "plugin:foo".into(),
            metric_key: metric.into(),
            location_key: None,
            instance_keys: "sql:local".into(),
            id,
            parameters,
        }
    };
    let mut instance = InMemoryInstance {
        pipes: vec![
            registered("shared", 1, &["alpha", "beta"]),
            registered("owned", 2, &["alpha"]),
        ],
        ..InMemoryInstance::default()
    };
    let out = run_with(
        &env,
        &["down"],
        &mut InMemoryJobs::default(),
        &mut instance,
        &mut RecordingRunner::default(),
        &mut NoInput,
        true,
        true,
        |_| {},
    );
    let result = out.result.unwrap();
    assert!(result.success, "{}", result.message);
    let edits: Vec<_> = instance
        .events
        .iter()
        .filter_map(|e| match e {
            InstanceEvent::Edit(pipe, params) => Some((pipe.clone(), params.clone())),
            _ => None,
        })
        .collect();
    let deletes: Vec<_> = instance
        .events
        .iter()
        .filter_map(|e| match e {
            InstanceEvent::Delete(pipe) => Some(pipe.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(edits.len(), 1, "{:?}", instance.events);
    assert!(edits[0].0.contains("shared"), "{:?}", edits);
    assert_eq!(
        edits[0].1.get("tags"),
        Some(&Value::List(vec![Value::Str("beta".into())]))
    );
    assert_eq!(deletes.len(), 1, "{:?}", instance.events);
    assert!(deletes[0].contains("owned"), "{deletes:?}");
}

#[test]
fn up_refuses_foreign_collisions_and_propagates_job_start_failures() {
    let env = env_from_yaml("project_name: alpha\nroot_dir: ./root\njobs:\n  api: start api\n");
    // A job of another project already has the name: refuse, and start nothing.
    let mut jobs = jobs_of(vec![(
        "api",
        job(
            &["start", "api", "--name", "api", "-t", "beta"],
            None,
            "running",
            false,
        ),
    )]);
    let o = scoped(&env, &["up"], &mut jobs, true);
    assert!(!succeeded(&o));
    assert!(message(&o).contains("not owned"), "{}", message(&o));
    assert!(
        !o.commands
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("start")),
        "{:?}",
        o.commands
    );

    // No collision: the job is started, and a failing start fails `up`.
    let mut runner = RecordingRunner::default();
    runner.results.push((
        vec!["start".to_string()],
        meerschaum_core::SuccessTuple::fail("start failed"),
    ));
    let out = run_with(
        &env,
        &["up"],
        &mut InMemoryJobs::default(),
        &mut InMemoryInstance::default(),
        &mut runner,
        &mut NoInput,
        true,
        false,
        |_| {},
    );
    let result = out.result.unwrap();
    assert!(!result.success);
    assert!(
        result.message.contains("start failed"),
        "{}",
        result.message
    );
}

#[test]
fn up_starts_jobs_instead_of_syncing_pipes() {
    let env = env_from_yaml(
        "project_name: alpha\nroot_dir: ./root\npipes:\n  - connector: sql:local\n    metric: bar\n    instance: sql:local\njobs:\n  hello: show version\n",
    );
    let o = scoped(&env, &["up"], &mut InMemoryJobs::default(), true);
    assert!(succeeded(&o), "{}", message(&o));
    assert!(
        message(&o).contains("Running 1 background job"),
        "{}",
        message(&o)
    );
    let ran = acting(&o.commands);
    assert!(
        ran.iter()
            .any(|c| c.contains(&"hello".to_string()) && c.contains(&"-d".to_string())),
        "the job was started: {ran:?}"
    );
    assert!(
        !ran.iter()
            .any(|c| c.starts_with(&["sync".to_string(), "pipes".to_string()])),
        "`up` must not sync the pipes when jobs are configured: {ran:?}"
    );

    // `--presync` (with `--no-jobs`) still runs a sync pass.
    let out = run_with(
        &env,
        &["up"],
        &mut InMemoryJobs::default(),
        &mut InMemoryInstance::default(),
        &mut RecordingRunner::default(),
        &mut NoInput,
        true,
        false,
        |args| {
            args.presync = true;
            args.no_jobs = true;
        },
    );
    let result = out.result.unwrap();
    assert!(result.success, "{}", result.message);
}

/// A runner which makes `install plugins <name>` do what it does: put the plugin where the
/// project's plugins directory shows it.
struct InstallingRunner {
    plugins_dir: PathBuf,
    commands: Vec<Vec<String>>,
}

impl meerschaum_compose::command::CommandRunner for InstallingRunner {
    fn run(
        &mut self,
        command: &meerschaum_compose::command::MrsmCommand,
    ) -> meerschaum_core::SuccessTuple {
        self.commands.push(command.sysargs.clone());
        if command
            .sysargs
            .starts_with(&["install".to_string(), "plugins".to_string()])
        {
            for name in command.sysargs[2..]
                .iter()
                .take_while(|a| !a.starts_with('-'))
            {
                std::fs::create_dir_all(&self.plugins_dir).unwrap();
                std::fs::write(self.plugins_dir.join(format!("{name}.py")), "").unwrap();
            }
        }
        meerschaum_core::SuccessTuple::success()
    }
}

/// v4's `test_compose_init_provisions_plugins_on_first_run` (a fixed regression): a fresh
/// project, with no plugins visible yet, still runs `install required` and `setup plugins` on
/// the FIRST `compose init`, once installing its plugins has made them visible.
#[test]
fn init_provisions_plugins_on_the_first_run() {
    let env = env_from_yaml(
        "project_name: fresh-project\nroot_dir: ./root\nplugins_dir: [./plugins]\nplugins: [proj_plugin]\n",
    );
    assert!(!env.project.join("plugins").exists());
    let mut runner = InstallingRunner {
        plugins_dir: env.project.join("plugins"),
        commands: Vec::new(),
    };
    let host = host(&env);
    let args = ComposeArgs {
        action: vec!["init".into()],
        sysargs: vec!["compose".into(), "init".into(), "-y".into()],
        yes: true,
        width: Some(100),
        ..ComposeArgs::default()
    };
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
    let result = out.result.unwrap();
    assert!(result.success, "{}", result.message);
    let ran = acting(&runner.commands);
    let position = |words: [&str; 2]| {
        ran.iter()
            .position(|c| c.starts_with(&[words[0].to_string(), words[1].to_string()]))
    };
    let installed = position(["install", "plugins"]).expect("the plugin was installed");
    let required = position(["install", "required"]).expect("`install required` ran");
    let setup = position(["setup", "plugins"]).expect("`setup plugins` ran");
    assert!(installed < required && required < setup, "{ran:?}");
}

/// rc15 (S4): `up` checks every job it would start before it registers or syncs anything: a
/// name another project's job has refuses the whole `up`, listing the names.
#[test]
fn up_refuses_foreign_collisions_before_registering_or_syncing() {
    let env = env_from_yaml(
        "project_name: alpha\nroot_dir: ./root\nsync:\n  min_seconds: 60\n\
         pipes:\n  - connector: plugin:noaa\n    metric: weather\n    instance: sql:local\n",
    );
    // `alpha sync (sql:local)` is the name of the job `up` starts for these pipes; another
    // project (`beta`) already has a job by that name.
    let mut jobs = jobs_of(vec![(
        "alpha sync (sql:local)",
        job(
            &["sync", "pipes", "-t", "beta"],
            Some("beta"),
            "running",
            true,
        ),
    )]);
    let mut instance = InMemoryInstance::default();
    let mut runner = RecordingRunner::default();
    let out = run_with(
        &env,
        &["up"],
        &mut jobs,
        &mut instance,
        &mut runner,
        &mut NoInput,
        true,
        false,
        |args| args.presync = true,
    );
    let result = out.result.unwrap();
    assert!(!result.success);
    assert!(
        result
            .message
            .contains("not owned by this Compose project: alpha sync (sql:local)"),
        "{}",
        result.message
    );
    assert!(
        result
            .message
            .contains("Nothing was registered, synced or started")
    );
    let acted: Vec<Vec<String>> = acting(
        &runner
            .commands
            .into_iter()
            .map(|c| c.sysargs)
            .collect::<Vec<_>>(),
    );
    assert!(
        !acted.iter().any(|c| matches!(
            c.first().map(String::as_str),
            Some("register" | "sync" | "start")
        )),
        "{acted:?}"
    );
    assert!(instance.events.is_empty(), "{:?}", instance.events);
}

/// rc15 (Q22/Q30): a project's `local` job that its systemd executor takes over is not deleted
/// before it starts again (the move stops it and keeps its logs); other owned jobs still are.
#[test]
fn up_starts_a_job_moving_to_systemd_without_deleting_it_first() {
    struct Moving(InMemoryJobs);
    impl meerschaum_compose::ops::JobOps for Moving {
        fn get_jobs(&mut self) -> indexmap::IndexMap<String, JobInfo> {
            self.0.get_jobs()
        }
        fn moves_to_systemd(&mut self, name: &str) -> bool {
            name == "worker"
        }
    }
    let env = env_from_yaml(ALPHA);
    let mut jobs = Moving(jobs_of(vec![
        (
            "worker",
            job(
                &["sync", "pipes", "--loop", "-t", "alpha"],
                Some("alpha"),
                "running",
                true,
            ),
        ),
        (
            "api",
            job(
                &["start", "api", "-t", "alpha"],
                Some("alpha"),
                "running",
                false,
            ),
        ),
    ]));
    let mut runner = RecordingRunner::default();
    let out = run_with(
        &env,
        &["up"],
        &mut jobs,
        &mut InMemoryInstance::default(),
        &mut runner,
        &mut NoInput,
        true,
        false,
        |_| {},
    );
    let result = out.result.unwrap();
    assert!(result.success, "{}", result.message);
    let commands: Vec<Vec<String>> = runner.commands.into_iter().map(|c| c.sysargs).collect();
    let deleted: Vec<&String> = commands
        .iter()
        .filter(|c| c.first().map(String::as_str) == Some("delete"))
        .filter_map(|c| c.get(2))
        .collect();
    assert_eq!(deleted, vec!["api"], "{commands:?}");
    assert!(
        commands
            .iter()
            .any(|c| c.windows(2).any(|w| w[0] == "--name" && w[1] == "worker")),
        "{commands:?}"
    );
    assert!(
        out.messages
            .items
            .iter()
            .any(|m| format!("{m:?}").contains("Moving job 'worker' to systemd")),
        "{:?}",
        out.messages.items
    );
}
