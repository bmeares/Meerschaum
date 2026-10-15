//! Native Compose execution over disposable SQLite roots; no Python core is available.
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use meerschaum_core::Pipe;
use meerschaum_jobs::{Job, JobsContext, Status};

struct Sandbox {
    temp: tempfile::TempDir,
}

impl Sandbox {
    fn new(body: &str) -> Self {
        let sandbox = Self {
            temp: tempfile::tempdir().unwrap(),
        };
        for dir in ["host", "root", "plugins", "host-plugins", "home"] {
            std::fs::create_dir(sandbox.path().join(dir)).unwrap();
        }
        sandbox.project("native", body);
        sandbox
    }

    fn path(&self) -> &Path {
        self.temp.path()
    }

    fn project(&self, name: &str, body: &str) {
        std::fs::write(self.path().join("mrsm-compose.yaml"), format!(
            "project_name: {name}\nroot_dir: ./root\nplugins_dir: [./plugins]\nconfig:\n  meerschaum:\n    instance: sql:local\n    compose_marker: {name}\n  system:\n    cli:\n      check_updates: false\n{body}\n"
        )).unwrap();
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_mrsm"))
            .args(args).current_dir(self.path()).env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.path().join("home"))
            .env("MRSM_ROOT_DIR", self.path().join("host"))
            .env("MRSM_PLUGINS_DIR", self.path().join("host-plugins"))
            .env("MRSM_PYTHON", self.path().join("no-python"))
            .env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:local"},"system":{"cli":{"check_updates":false}}}"#)
            .output().unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn context(&self) -> JobsContext {
        JobsContext::for_root(self.path().join("root"))
    }

    fn wait_stopped(&self, name: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut job = Job::open(name, &self.context()).unwrap();
            if job.status() == Status::Stopped {
                return;
            }
            assert!(Instant::now() < deadline, "job {name} did not stop");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        for (_, mut job) in meerschaum_jobs::executor::get_jobs(&self.context(), true) {
            let _ = job.delete();
        }
    }
}

#[test]
fn native_up_ps_logs_replacement_and_down_preserve_foreign_jobs() {
    let body = "jobs:\n  compose-smoke: show version";
    let sandbox = Sandbox::new(body);
    sandbox.ok(&["compose", "up"]);
    sandbox.wait_stopped("compose-smoke");
    let mut job = Job::open("compose-smoke", &sandbox.context()).unwrap();
    assert_eq!(
        job.env().get("MRSM__COMPOSE_PROJECT").map(String::as_str),
        Some("native")
    );
    assert!(job.sysargs().contains(&"version".into()));
    assert!(
        sandbox
            .ok(&["compose", "ps", "--nopretty"])
            .contains("compose-smoke")
    );
    assert!(
        sandbox
            .ok(&["compose", "logs", "--nopretty"])
            .contains("MRSM_JOB: compose-smoke")
    );
    sandbox.ok(&["compose", "up"]);
    sandbox.wait_stopped("compose-smoke");

    sandbox.project("other", body);
    let collision = sandbox.run(&["compose", "up"]);
    assert!(!collision.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&collision.stdout),
        String::from_utf8_lossy(&collision.stderr)
    );
    assert!(text.contains("Refusing to replace"), "{text}");
    sandbox.ok(&["compose", "down"]);
    assert!(sandbox.path().join("root/jobs/compose-smoke").exists());
    sandbox.project("native", body);
    sandbox.ok(&["compose", "down"]);
    assert!(!sandbox.path().join("root/jobs/compose-smoke").exists());
    assert!(!sandbox.path().join("host/jobs").exists());
}

#[test]
fn native_presync_and_down_volumes_preserve_shared_data() {
    let sandbox = Sandbox::new(
        "pipes:\n  - connector: sql:local\n    metric: compose_rows\n    parameters:\n      sql: SELECT 1 AS id, 'sample' AS value\n      columns: {id: id}\n      target: compose_rows\n  - connector: sql:local\n    metric: shared_rows\n    parameters:\n      sql: SELECT 2 AS id\n      columns: {id: id}\n      target: shared_rows",
    );
    sandbox.ok(&["compose", "up", "--presync", "--no-jobs"]);
    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                sandbox.path().join("root").display().to_string(),
            ),
            (
                "MRSM_PLUGINS_DIR".into(),
                sandbox.path().join("plugins").display().to_string(),
            ),
        ]))
        .unwrap();
    let session = meerschaum_cli::native::Session::new();
    let instance = session.instance("sql:local", &config).unwrap();
    let pipe = Pipe::builder("sql:local", "compose_rows")
        .instance("sql:local")
        .build()
        .unwrap();
    assert!(
        session
            .block_on(instance.conn().get_pipe_id(&pipe))
            .unwrap()
            .is_some()
    );
    let sql = instance.sql().unwrap();
    let count = session
        .block_on(sql.driver().value("SELECT COUNT(*) AS n FROM compose_rows"))
        .unwrap();
    assert_eq!(count.unwrap().as_i64(), Some(1));
    let mut shared = Pipe::builder("sql:local", "shared_rows")
        .instance("sql:local")
        .build()
        .unwrap();
    let attributes = session
        .block_on(instance.conn().get_pipe_attributes(&shared))
        .unwrap();
    let mut parameters = attributes["parameters"].to_map();
    parameters.insert(
        "tags".into(),
        meerschaum_core::Value::List(vec![
            meerschaum_core::Value::Str("native".into()),
            meerschaum_core::Value::Str("other".into()),
        ]),
    );
    shared.set_parameters(parameters);
    assert!(
        session
            .block_on(instance.conn().edit_pipe(&shared, false))
            .success
    );
    sandbox.ok(&["compose", "down", "-v", "-y"]);
    assert!(
        session
            .block_on(instance.conn().get_pipe_id(&pipe))
            .unwrap()
            .is_none()
    );
    assert!(
        session
            .block_on(sql.driver().value("SELECT COUNT(*) FROM compose_rows"))
            .is_err()
    );
    assert!(
        session
            .block_on(instance.conn().get_pipe_id(&shared))
            .unwrap()
            .is_some()
    );
    let attributes = session
        .block_on(instance.conn().get_pipe_attributes(&shared))
        .unwrap();
    assert_eq!(
        attributes["parameters"].to_map()["tags"],
        meerschaum_core::Value::List(vec![meerschaum_core::Value::Str("other".into())])
    );
    assert_eq!(
        session
            .block_on(sql.driver().value("SELECT COUNT(*) FROM shared_rows"))
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(1)
    );
    session.shutdown();
}

#[test]
fn native_temporary_pipe_sync_never_registers() {
    let sandbox = Sandbox::new(
        "pipes:\n  - connector: sql:local\n    metric: temporary_rows\n    temporary: true\n    parameters:\n      sql: SELECT 7 AS id\n      columns: {id: id}\n      target: temporary_rows",
    );
    sandbox.ok(&["compose", "run"]);
    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                sandbox.path().join("root").display().to_string(),
            ),
            (
                "MRSM_PLUGINS_DIR".into(),
                sandbox.path().join("plugins").display().to_string(),
            ),
        ]))
        .unwrap();
    let session = meerschaum_cli::native::Session::new();
    let instance = session.instance("sql:local", &config).unwrap();
    let pipe = Pipe::builder("sql:local", "temporary_rows")
        .instance("sql:local")
        .build()
        .unwrap();
    assert!(
        session
            .block_on(instance.conn().get_pipe_id(&pipe))
            .unwrap()
            .is_none()
    );
    let count = session
        .block_on(
            instance
                .sql()
                .unwrap()
                .driver()
                .value("SELECT COUNT(*) FROM temporary_rows"),
        )
        .unwrap();
    assert_eq!(count.unwrap().as_i64(), Some(1));
    session.shutdown();
}

#[test]
fn temporary_python_plugin_receives_project_environment() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let python = std::env::var("MRSM_TEST_COMPOSE_PYTHON").ok().or_else(|| {
        [
            repo.join(".venv/bin/python"),
            repo.join(".venv-ref/bin/python"),
        ]
        .into_iter()
        .find(|path| path.is_file())
        .map(|path| path.display().to_string())
    });
    let Some(python) = python else {
        eprintln!(
            "skipped: no local or reference Python environment; set MRSM_TEST_COMPOSE_PYTHON"
        );
        return;
    };
    let sandbox = Sandbox::new(&format!(
        "environment:\n  MRSM_PYTHON: {}\n  PYTHONPATH: {}\n  COMPOSE_SAMPLE: '37'\npipes:\n  - connector: plugin:compose_sample\n    metric: sample\n    temporary: true\n    parameters:\n      columns: {{id: id}}\n      target: compose_plugin_sample",
        serde_json::to_string(&python).unwrap(),
        serde_json::to_string(&repo.display().to_string()).unwrap(),
    ));
    std::fs::write(sandbox.path().join("plugins/compose_sample.py"), "import os\ndef fetch(pipe, **kw):\n    assert os.environ['COMPOSE_SAMPLE'] == '37'\n    assert os.environ['MRSM__COMPOSE_PROJECT'] == 'native'\n    from meerschaum.config import get_config\n    assert get_config('meerschaum', 'compose_marker') == 'native'\n    return [{'id': 37}]\n").unwrap();
    sandbox.ok(&["compose", "run"]);
    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                sandbox.path().join("root").display().to_string(),
            ),
            (
                "MRSM_PLUGINS_DIR".into(),
                sandbox.path().join("host-plugins").display().to_string(),
            ),
        ]))
        .unwrap();
    let session = meerschaum_cli::native::Session::new();
    let instance = session.instance("sql:local", &config).unwrap();
    let value = session
        .block_on(
            instance
                .sql()
                .unwrap()
                .driver()
                .value("SELECT id FROM compose_plugin_sample"),
        )
        .unwrap();
    assert_eq!(value.unwrap().as_i64(), Some(37));
    session.shutdown();
}

#[test]
fn reusing_instance_adapter_replaces_connectors_and_worker_scope() {
    use meerschaum_compose::ops::InstanceOps;
    let first = Sandbox::new("");
    let second = Sandbox::new("");
    let mut adapter = meerschaum_cli::native::compose_instance::ComposeInstance::new();
    for (sandbox, value) in [(&first, 1), (&second, 2)] {
        let host = meerschaum_compose::Host::new(
            sandbox.path(),
            vec![
                (
                    "MRSM_ROOT_DIR".into(),
                    sandbox.path().join("host").display().to_string(),
                ),
                (
                    "MRSM_PLUGINS_DIR".into(),
                    sandbox.path().join("host-plugins").display().to_string(),
                ),
                (
                    "HOME".into(),
                    sandbox.path().join("home").display().to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
        let project = meerschaum_compose::project::load_project(
            &host,
            None,
            None,
            false,
            &mut Default::default(),
        )
        .unwrap();
        adapter.try_configure(&host, &project).unwrap();
        let mut pipe = Pipe::builder("sql:local", "scope_rows").instance("sql:local").parameters(
            meerschaum_core::Value::from_json_str(&format!(r#"{{"sql":"SELECT {value} AS id", "columns":{{"id":"id"}},"target":"scope_rows"}}"#)).unwrap().to_map()
        ).build().unwrap();
        pipe.temporary = true;
        let result = adapter.sync_pipe(&pipe);
        assert!(result.success, "{}", result.message);
        let config = meerschaum_compose::pipes::ProjectConfig::new(&host, &project).unwrap();
        let session = meerschaum_cli::native::Session::new();
        let instance = session.instance("sql:local", config.registry()).unwrap();
        let actual = session
            .block_on(
                instance
                    .sql()
                    .unwrap()
                    .driver()
                    .value("SELECT id FROM scope_rows"),
            )
            .unwrap();
        assert_eq!(actual.unwrap().as_i64(), Some(value));
        session.shutdown();
    }
}

#[test]
fn temporary_definition_preserves_an_existing_registration_on_down() {
    let definition = "pipes:\n  - connector: sql:local\n    metric: protected_rows\n    parameters:\n      sql: SELECT 5 AS id\n      columns: {id: id}\n      target: protected_rows";
    let sandbox = Sandbox::new(definition);
    sandbox.ok(&["compose", "up", "--presync", "--no-jobs"]);
    sandbox.project(
        "native",
        &definition.replace(
            "metric: protected_rows",
            "metric: protected_rows\n    temporary: true",
        ),
    );
    let output = sandbox.run(&["compose", "down", "-v", "-y"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("No pipes to delete"));
    // Restoring the ordinary definition must still see the original registration and data.
    sandbox.project("native", definition);
    let explain = sandbox.ok(&["compose", "explain"]);
    assert!(explain.contains("Up-to-date"), "{explain}");
}
