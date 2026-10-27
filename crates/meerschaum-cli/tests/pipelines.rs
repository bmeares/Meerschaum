//! Standalone registered Pipeline lifecycle, approval boundaries and exact cross-instance refs.
use serde_json::{Value, json};
use std::path::PathBuf;
use std::process::{Command, Output};

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
}
impl Env {
    fn new() -> Self {
        let base = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap()
            .join(".cache/mrsm-pipeline-tests");
        std::fs::create_dir_all(&base).unwrap();
        let tmp = tempfile::tempdir_in(base).unwrap();
        let root = tmp.path().join("root");
        let plugins = tmp.path().join("plugins");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&plugins).unwrap();
        Self {
            _tmp: tmp,
            root,
            plugins,
        }
    }
    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") || ["TMUX", "TMUX_PANE", "RUST_LOG"].contains(&key.as_str())
            {
                command.env_remove(key);
            }
        }
        command.env("MRSM_ROOT_DIR", &self.root).env("MRSM_PLUGINS_DIR", &self.plugins)
            .env("MRSM_NOASK", "true").env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_SQL_REMOTE", format!("sqlite:///{}", self.root.join("remote.db").display()))
            .env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:local"},"formatting":{"ansi":false,"unicode":false},"shell":{"updates":{"check":false}}}"#)
            .args(args).output().unwrap()
    }
    fn ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        let text = String::from_utf8_lossy(&output.stdout).to_string();
        assert!(
            output.status.success(),
            "{args:?}\n{text}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        text
    }
    fn register_pipe(&self, metric: &str, instance: &str, params: Value) {
        self.ok(&[
            "register",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            metric,
            "-i",
            instance,
            "--params",
            &params.to_string(),
            "-y",
        ]);
    }
    fn register_pipeline(&self, name: &str, params: Value) {
        self.ok(&[
            "register",
            "pipelines",
            name,
            "--params",
            &params.to_string(),
            "-i",
            "sql:local",
            "-y",
        ]);
    }
    fn definitions(&self) -> Value {
        let output = self.ok(&["show", "pipelines", "--nopretty", "-i", "sql:local"]);
        serde_json::from_str(&output).expect("show pipelines --nopretty is one JSON document")
    }
    fn pipe_parameters(&self, metric: &str, instance: &str) -> Value {
        let output = self.ok(&[
            "show",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            metric,
            "-i",
            instance,
            "--nopretty",
        ]);
        let value: Value = serde_json::Deserializer::from_str(&output)
            .into_iter()
            .next()
            .unwrap()
            .unwrap();
        value["parameters"].clone()
    }
}

#[test]
fn registered_lifecycle_is_native_and_does_not_apply_tags_on_reads() {
    let env = Env::new();
    env.register_pipe(
        "a",
        "sql:local",
        json!({"columns":{"id":"id"},"sql":"SELECT 1 AS id"}),
    );
    env.register_pipe(
        "b",
        "sql:local",
        json!({"columns":{"id":"id"},"sql":"SELECT 2 AS id"}),
    );
    env.register_pipeline(
        "daily",
        json!({"targets":["sql:local|sql:local|a|None"],"description":"demo"}),
    );
    assert!(
        !env.run(&["register", "pipeline", "daily", "-y"])
            .status
            .success()
    );
    let definitions = env.definitions();
    let id = definitions[0]["id"].clone();
    assert_eq!(definitions[0]["revision"], 1);
    env.ok(&["plan", "pipeline", "daily", "--nopretty"]);
    env.ok(&["analyze", "pipelines", "daily", "--nopretty"]);
    assert!(env.pipe_parameters("a", "sql:local")["tags"].is_null());
    env.ok(&["analyze", "pipelines", "daily", "--yes"]);
    assert_eq!(
        env.pipe_parameters("a", "sql:local")["tags"],
        json!(["pipeline-daily"])
    );
    env.ok(&["copy", "pipelines", "daily", "daily-copy"]);
    let copied = env.definitions();
    assert_ne!(copied[0]["id"], copied[1]["id"]);
    assert!(
        !env.run(&[
            "edit",
            "pipelines",
            "daily",
            "--params",
            r#"{"revision":0,"targets":[]}"#
        ])
        .status
        .success()
    );
    env.ok(&[
        "edit",
        "pipelines",
        "daily",
        "--params",
        r#"{"revision":1,"targets":["sql:local|sql:local|b|None"]}"#,
    ]);
    assert_eq!(env.definitions()[0]["id"], id);
    env.ok(&["analyze", "pipelines", "daily", "--yes"]);
    assert_eq!(env.pipe_parameters("a", "sql:local")["tags"], json!([]));
    assert_eq!(
        env.pipe_parameters("b", "sql:local")["tags"],
        json!(["pipeline-daily"])
    );
    env.ok(&["delete", "pipelines", "daily", "-y"]);
    assert_eq!(env.pipe_parameters("b", "sql:local")["tags"], json!([]));
}

#[test]
fn show_pipelines_reads_saved_members_without_traversal() {
    let env = Env::new();
    env.register_pipe("source", "sql:remote", json!({"sql":"SELECT 1 AS id"}));
    env.register_pipe("report", "sql:local", json!({"parents":[{"connector":"sql:local","metric":"source","instance":"sql:remote"}],"sql":"SELECT 1 AS id"}));
    env.register_pipeline("daily", json!({"description":"Daily reports", "targets":["sql:local|sql:local|report|None"],"parents":true}));
    let before = env.definitions();
    let output = env.ok(&["show", "pipelines"]);
    assert!(output.contains("Pipeline('daily')"), "{output}");
    assert!(output.contains("Daily reports"), "{output}");
    assert!(
        output.contains("1 member") && !output.contains("1 members"),
        "{output}"
    );
    assert!(
        !output.contains("source"),
        "show must not traverse: {output}"
    );
    assert!(output.contains("Keys"), "{output}");
    assert!(!output.contains("schema_version"), "{output}");
    assert!(!output.contains("Revision:"), "{output}");
    let detail = env.ok(&["show", "pipeline", "daily"]);
    assert!(
        detail.contains("Revision: 1") && detail.contains("Parents: true"),
        "{detail}"
    );
    assert_eq!(env.definitions(), before);
    assert!(env.pipe_parameters("source", "sql:remote")["tags"].is_null());
    assert!(env.pipe_parameters("report", "sql:local")["tags"].is_null());
    let runs = env.ok(&["show", "runs", "--nopretty"]);
    assert_eq!(serde_json::from_str::<Value>(&runs).unwrap(), json!([]));
}

#[test]
fn show_pipelines_keeps_missing_members_visible_and_handles_empty_registries() {
    let env = Env::new();
    assert!(
        env.ok(&["show", "pipelines"])
            .contains("No pipelines registered on sql:local.")
    );
    env.register_pipeline(
        "missing",
        json!({"targets":["sql:remote|sql:local|absent|None"]}),
    );
    env.register_pipeline("empty", json!({"targets":[]}));
    let output = env.ok(&["show", "pipelines"]);
    assert!(!output.contains("Connections unavailable:"), "{output}");
    assert!(
        env.ok(&["show", "pipeline", "missing"])
            .contains("sql:remote|sql:local|absent|None"),
        "{output}"
    );
    assert!(output.contains("No member pipes."), "{output}");
    assert_eq!(env.definitions().as_array().unwrap().len(), 2);
}

#[test]
fn cross_instance_same_triples_and_preexisting_tags_survive() {
    let env = Env::new();
    env.register_pipe(
        "same",
        "sql:local",
        json!({"tags":["pipeline-cross"],"sql":"SELECT 1 AS id"}),
    );
    env.register_pipe("same", "sql:remote", json!({"sql":"SELECT 2 AS id"}));
    env.register_pipeline(
        "cross",
        json!({"targets":["sql:local|sql:local|same|None","sql:remote|sql:local|same|None"]}),
    );
    let output = env.ok(&["analyze", "pipeline", "cross", "--yes"]);
    assert!(output.contains("sql:remote|sql:local|same|None"));
    env.ok(&["delete", "pipeline", "cross", "--yes"]);
    assert_eq!(
        env.pipe_parameters("same", "sql:local")["tags"],
        json!(["pipeline-cross"])
    );
    assert_eq!(env.pipe_parameters("same", "sql:remote")["tags"], json!([]));
}

#[test]
fn missing_member_blocks_analysis_and_sync_without_registering_it() {
    let env = Env::new();
    env.register_pipeline(
        "missing",
        json!({"targets":["sql:local|sql:local|absent|None"]}),
    );
    assert!(
        !env.run(&["analyze", "pipelines", "missing", "--yes"])
            .status
            .success()
    );
    assert!(
        !env.run(&["sync", "pipelines", "missing", "--yes"])
            .status
            .success()
    );
    assert!(
        !env.run(&["drop", "pipelines", "missing", "--yes"])
            .status
            .success()
    );
}

#[test]
fn explicit_run_id_preserves_pipeline_identity() {
    let env = Env::new();
    env.register_pipe(
        "a",
        "sql:local",
        json!({"columns":{"id":"id"},"sql":"SELECT 1 AS id"}),
    );
    env.register_pipeline("daily", json!({"targets":["sql:local|sql:local|a|None"]}));
    let output = env.ok(&[
        "sync",
        "pipelines",
        "daily",
        "--run-id",
        "ui-run-42",
        "--yes",
        "--nopretty",
    ]);
    assert!(output.contains("Run: ui-run-42"));
    let record = env.ok(&["show", "run", "ui-run-42", "--nopretty"]);
    assert!(record.contains("pipeline_definitions"));
    assert!(record.contains("daily"));
    let duplicate = env.run(&[
        "sync",
        "pipelines",
        "daily",
        "--run-id",
        "ui-run-42",
        "--yes",
    ]);
    assert!(!duplicate.status.success());
    assert_eq!(
        record,
        env.ok(&["show", "run", "ui-run-42", "--nopretty"]),
        "duplicate run id must preserve immutable history"
    );
    assert!(
        !env.run(&["sync", "pipelines", "daily", "--run-id", "bad/id"])
            .status
            .success()
    );
}

#[test]
fn recursive_cross_instance_dependencies_are_discovered_before_tag_approval() {
    let env = Env::new();
    env.register_pipe("grandparent", "sql:local", json!({"sql":"SELECT 1 AS id"}));
    env.register_pipe("parent", "sql:remote", json!({"parents":[{"connector":"sql:local","metric":"grandparent","instance":"sql:local"}],"sql":"SELECT 1 AS id"}));
    env.register_pipe("child", "sql:local", json!({"parents":[{"connector":"sql:local","metric":"parent","instance":"sql:remote"}],"sql":"SELECT 1 AS id"}));
    env.register_pipeline(
        "recursive",
        json!({"targets":["sql:local|sql:local|child|None"],"parents":true}),
    );
    let preview = env.ok(&["analyze", "pipelines", "recursive", "--nopretty"]);
    assert!(preview.contains("sql:remote|sql:local|parent|None"));
    assert!(preview.contains("sql:local|sql:local|grandparent|None"));
    assert!(env.pipe_parameters("parent", "sql:remote")["tags"].is_null());
    env.ok(&["analyze", "pipelines", "recursive", "--yes"]);
    assert_eq!(
        env.pipe_parameters("grandparent", "sql:local")["tags"],
        json!(["pipeline-recursive"])
    );
    assert_eq!(
        env.pipe_parameters("parent", "sql:remote")["tags"],
        json!(["pipeline-recursive"])
    );
}

#[test]
fn missing_traversal_reference_blocks_tag_mutation() {
    let env = Env::new();
    env.register_pipe("child", "sql:local", json!({"parents":[{"connector":"sql:local","metric":"gone","instance":"sql:remote"}],"sql":"SELECT 1 AS id"}));
    env.register_pipeline(
        "incomplete",
        json!({"targets":["sql:local|sql:local|child|None"],"parents":true}),
    );
    assert!(
        !env.run(&["analyze", "pipelines", "incomplete", "--yes"])
            .status
            .success()
    );
    assert!(env.pipe_parameters("child", "sql:local")["tags"].is_null());
}

#[test]
fn combined_defaults_require_an_explicit_override_when_they_conflict() {
    let env = Env::new();
    env.register_pipe("a", "sql:local", json!({"sql":"SELECT 1 AS id"}));
    env.register_pipeline(
        "a",
        json!({"targets":["sql:local|sql:local|a|None"],"defaults":{"workers":1}}),
    );
    env.register_pipeline(
        "b",
        json!({"targets":["sql:local|sql:local|a|None"],"defaults":{"workers":2}}),
    );
    assert!(!env.run(&["plan", "pipelines", "a", "b"]).status.success());
    env.ok(&["plan", "pipelines", "a", "b", "--workers", "3"]);
}

#[test]
fn importing_an_exported_definition_creates_a_new_identity() {
    let env = Env::new();
    env.register_pipe("a", "sql:local", json!({"sql":"SELECT 1 AS id"}));
    env.register_pipeline("original", json!({"targets":["sql:local|sql:local|a|None"],"layout":{"positions":{"a":[10,20]}},"defaults":{"workers":2}}));
    let exported = env.definitions()[0].clone();
    env.register_pipeline("imported", exported.clone());
    let imported = env.definitions()[0].clone();
    assert_ne!(imported["id"], exported["id"]);
    assert_eq!(imported["revision"], json!(1));
    assert_eq!(imported["targets"], exported["targets"]);
    assert_eq!(imported["layout"], exported["layout"]);
}

#[test]
fn delete_pipeline_selection_skips_missing_names_and_bare_delete_selects_all() {
    let env = Env::new();
    env.register_pipe("kept", "sql:local", json!({}));
    env.register_pipeline(
        "first",
        json!({"targets":["sql:local|sql:local|kept|None"]}),
    );
    env.register_pipeline("second", json!({"targets":[]}));
    let missing = env.run(&["delete", "pipelines", "missing", "--yes"]);
    assert!(!missing.status.success());
    // The error is printed once, in the failure block (stdout).
    assert!(String::from_utf8_lossy(&missing.stdout).contains("No valid Pipeline"));
    assert_eq!(env.definitions().as_array().unwrap().len(), 2);
    let mixed = env.run(&["delete", "pipelines", "missing", "first", "--yes"]);
    assert!(
        mixed.status.success(),
        "{}",
        String::from_utf8_lossy(&mixed.stderr)
    );
    assert!(String::from_utf8_lossy(&mixed.stderr).contains("skipping"));
    assert_eq!(env.definitions().as_array().unwrap().len(), 1);
    env.ok(&["delete", "pipelines", "--yes"]);
    assert_eq!(env.definitions(), json!([]));
    assert!(env.pipe_parameters("kept", "sql:local").is_object());
    assert!(!env.run(&["delete", "pipelines", "--yes"]).status.success());
}
