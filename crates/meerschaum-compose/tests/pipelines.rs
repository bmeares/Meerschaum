use meerschaum_compose::command::RecordingRunner;
use meerschaum_compose::error::Messages;
use meerschaum_compose::ops::{InMemoryInstance, InMemoryJobs, InstanceOps, NoInput};
use meerschaum_compose::pipes::ProjectConfig;
use meerschaum_compose::project::{Host, Session, load_project};
use meerschaum_compose::subactions::{ComposeArgs, Services, compose};
use meerschaum_core::Value;
use std::path::PathBuf;

struct Fixture {
    _temp: tempfile::TempDir,
    dir: PathBuf,
    host: Host,
}
impl Fixture {
    fn new(body: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().canonicalize().unwrap();
        for path in ["home", "host-root", "plugins"] {
            std::fs::create_dir(dir.join(path)).unwrap();
        }
        std::fs::write(dir.join("mrsm-compose.yaml"),format!("project_name: example\nroot_dir: ./root\nconfig:\n  meerschaum:\n    instance: sql:local\n{body}")).unwrap();
        let env = [
            ("HOME".into(), dir.join("home").to_string_lossy().into()),
            (
                "MRSM_ROOT_DIR".into(),
                dir.join("host-root").to_string_lossy().into(),
            ),
        ]
        .into();
        let host = Host::new(&dir, env).unwrap();
        Self {
            _temp: temp,
            dir,
            host,
        }
    }
    fn definitions(&self) -> Result<Vec<meerschaum_compose::pipelines::PipelineSpec>, String> {
        let project =
            load_project(&self.host, None, None, false, &mut Messages::default()).unwrap();
        let pc = ProjectConfig::new(&self.host, &project).unwrap();
        meerschaum_compose::pipelines::defined(&project, &pc).map_err(|e| e.message)
    }
    fn run(
        &self,
        action: &str,
        dry: bool,
        instance: &mut InMemoryInstance,
    ) -> meerschaum_compose::ComposeOutput {
        compose(
            &self.host,
            &mut Session::default(),
            &ComposeArgs {
                action: vec![action.into()],
                dry,
                ..Default::default()
            },
            Services {
                runner: &mut RecordingRunner::default(),
                instance,
                jobs: &mut InMemoryJobs::default(),
                prompter: &mut NoInput,
            },
        )
    }
}
const DECLARATION: &str = "pipelines:\n  daily:\n    description: Daily totals\n    pipes:\n      - {connector: 'sql:source', metric: raw}\n      - {connector: 'sql:local', metric: totals, instance: 'sql:warehouse'}\n    defaults: {workers: 1}\n";

#[test]
fn exact_members_keep_the_project_instance_when_the_registry_changes() {
    let fixture = Fixture::new(&DECLARATION.replace(
        "description: Daily totals",
        "instance: 'sql:registry'\n    description: Daily totals",
    ));
    let specs = fixture.definitions().unwrap();
    assert_eq!(specs[0].instance, "sql:registry");
    assert_eq!(
        specs[0].definition["targets"],
        Value::List(vec![
            Value::from("sql:local|sql:source|raw|None"),
            Value::from("sql:warehouse|sql:local|totals|None")
        ])
    );
    assert_eq!(specs[0].definition["parents"], Value::Bool(false));
}

#[test]
fn explain_lists_members_without_creating_a_root_or_registration() {
    let fixture = Fixture::new(DECLARATION);
    let mut instance = InMemoryInstance::default();
    let output = fixture.run("explain", false, &mut instance);
    assert!(
        output.result.as_ref().unwrap().success,
        "{:?}",
        output.result
    );
    assert!(
        output
            .explain
            .as_ref()
            .unwrap()
            .contains("Pipelines in 'example'")
    );
    assert_eq!(output.explain_models[0].pipelines.len(), 1);
    assert!(instance.pipes.is_empty() && instance.events.is_empty());
    assert!(output.commands.is_empty());
    assert!(!fixture.dir.join("root").exists());
}

#[test]
fn dry_apply_is_idempotent_and_an_edit_keeps_identity_and_private_metadata() {
    let fixture = Fixture::new(DECLARATION);
    let mut instance = InMemoryInstance::default();
    for _ in 0..2 {
        let output = fixture.run("up", true, &mut instance);
        assert!(
            output.result.as_ref().unwrap().success,
            "{:?}",
            output.result
        );
    }
    assert_eq!(instance.pipes.len(), 1);
    let saved = &mut instance.pipes[0].parameters;
    assert_eq!(saved["pipeline"].get("revision"), Some(&Value::Int(1)));
    let identity = saved["pipeline"].get("id").unwrap().clone();
    saved.insert(
        "access".into(),
        Value::from_json_str(r#"{"visibility":"private","owner":"alice"}"#).unwrap(),
    );
    saved.insert(
        "locks".into(),
        Value::from_json_str(r#"{"sync":true}"#).unwrap(),
    );
    let mut definition = saved["pipeline"].to_map();
    definition.insert(
        "layout".into(),
        Value::from_json_str(r#"{"node":{"x":12}}"#).unwrap(),
    );
    saved.insert("pipeline".into(), Value::Map(definition));
    let path = fixture.dir.join("mrsm-compose.yaml");
    std::fs::write(
        &path,
        std::fs::read_to_string(&path)
            .unwrap()
            .replace("Daily totals", "Updated description"),
    )
    .unwrap();
    let output = fixture.run("up", true, &mut instance);
    assert!(
        output.result.as_ref().unwrap().success,
        "{:?}",
        output.result
    );
    let saved = &instance.pipes[0].parameters;
    assert_eq!(saved["pipeline"].get("id"), Some(&identity));
    assert_eq!(saved["pipeline"].get("revision"), Some(&Value::Int(2)));
    assert_eq!(
        saved["pipeline"]
            .get("layout")
            .unwrap()
            .get("node")
            .unwrap()
            .get("x"),
        Some(&Value::Int(12))
    );
    assert_eq!(saved["access"].get("owner"), Some(&Value::from("alice")));
    assert_eq!(saved["locks"].get("sync"), Some(&Value::Bool(true)));
    assert!(instance.synced.is_empty());
}

#[test]
fn stale_preflight_revision_cannot_overwrite_a_concurrent_edit() {
    let fixture = Fixture::new(DECLARATION);
    let spec = fixture.definitions().unwrap().remove(0);
    let mut instance = InMemoryInstance::default();
    assert!(instance.save_pipeline(&spec, "example", None).success);
    let before = instance.pipes.clone();
    assert!(!instance.save_pipeline(&spec, "example", None).success);
    assert_eq!(instance.pipes, before);
    let saved = instance.pipes[0].parameters.clone();
    assert!(spec.check_owner(Some(&saved), "different-project").is_err());
}

#[test]
fn unmanaged_name_collision_fails_before_registration_commands() {
    let fixture = Fixture::new(DECLARATION);
    let spec = fixture.definitions().unwrap().remove(0);
    let mut instance = InMemoryInstance::default();
    assert!(instance.save_pipeline(&spec, "example", None).success);
    instance.pipes[0].parameters.shift_remove("compose");
    instance.events.clear();
    let output = fixture.run("up", true, &mut instance);
    assert!(format!("{:?}", output.result).contains("not owned"));
    assert!(output.commands.is_empty() && instance.events.is_empty());
    assert!(!fixture.dir.join("root").exists());
}

#[test]
fn a_parameter_lock_refuses_changes_during_preflight() {
    let fixture = Fixture::new(DECLARATION);
    let spec = fixture.definitions().unwrap().remove(0);
    let mut instance = InMemoryInstance::default();
    assert!(instance.save_pipeline(&spec, "example", None).success);
    instance.pipes[0]
        .parameters
        .insert("locks".into(), Value::List(vec![Value::from("parameters")]));
    instance.events.clear();
    let file = fixture.dir.join("mrsm-compose.yaml");
    std::fs::write(
        &file,
        std::fs::read_to_string(&file)
            .unwrap()
            .replace("Daily totals", "Changed"),
    )
    .unwrap();
    let before = instance.pipes.clone();
    let output = fixture.run("up", true, &mut instance);
    assert!(format!("{:?}", output.result).contains("locked"));
    assert!(output.commands.is_empty() && instance.events.is_empty());
    assert_eq!(instance.pipes, before);
}

#[test]
fn duplicate_names_in_included_projects_fail_before_any_write() {
    let fixture = Fixture::new(&format!("include: [other.yaml]\n{DECLARATION}"));
    std::fs::write(fixture.dir.join("other.yaml"),format!("project_name: other\nroot_dir: ./root\nconfig:\n  meerschaum:\n    instance: sql:local\n{DECLARATION}")).unwrap();
    let mut instance = InMemoryInstance::default();
    let output = fixture.run("up", true, &mut instance);
    assert!(format!("{:?}", output.result).contains("declared by both"));
    assert!(output.commands.is_empty() && instance.pipes.is_empty());
}

#[test]
fn malformed_declarations_never_initialize_the_project() {
    let cases = [
        ("pipelines: []", "expected a mapping"),
        ("pipelines:\n  daily: {pipes: []}", "nonempty list"),
        (
            "pipelines:\n  daily: {pipes: [{connector: mrsm, metric: engine_nodes}]}",
            "internal metadata",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: [a,b], metric: raw}]}",
            "nonempty string",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw, tags: [x]}]}",
            "keys only",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw}], schedule: daily}",
            "unknown Pipeline field",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw}], defaults: {instance: 'sql:other'}}",
            "unsupported execution default",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw}], parents: -1}",
            "traversal depth",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw}], defaults: {workers: false}}",
            "invalid execution default",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw}], defaults: {retries: -1}}",
            "invalid execution default",
        ),
        (
            "pipelines:\n  daily: {pipes: [{connector: a, metric: raw}, {connector: a, metric: raw, location: null}]}",
            "duplicate member",
        ),
    ];
    for (body, message) in cases {
        let fixture = Fixture::new(body);
        let mut instance = InMemoryInstance::default();
        let output = fixture.run("up", true, &mut instance);
        assert!(
            format!("{:?}", output.result).contains(message),
            "{body}: {:?}",
            output.result
        );
        assert!(output.commands.is_empty() && instance.pipes.is_empty());
        assert!(!fixture.dir.join("root").exists());
    }
}
