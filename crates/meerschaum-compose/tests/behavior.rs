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

    let (codes, _) = lint_codes("basic");
    assert!(codes.is_empty(), "{codes:?}");
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
