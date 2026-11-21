//! Wave 18 Compose (design decision D22): `include:` / `parent:` trees, the `--file` flag in any
//! position, missing references, a side-effect-free `explain` with v4's colors, and exit codes,
//! over disposable SQLite roots and the native `mrsm-cli` binary.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use meerschaum_jobs::{Job, JobsContext};

struct Tree {
    temp: tempfile::TempDir,
}

impl Tree {
    fn new(files: &[(&str, &str)]) -> Tree {
        let tree = Tree {
            temp: tempfile::tempdir().unwrap(),
        };
        for dir in ["host", "host-plugins", "home"] {
            std::fs::create_dir(tree.path().join(dir)).unwrap();
        }
        for (path, text) in files {
            let file = tree.path().join("work").join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, text).unwrap();
        }
        tree
    }

    fn path(&self) -> &Path {
        self.temp.path()
    }

    fn work(&self) -> PathBuf {
        self.path().join("work")
    }

    fn run_in(&self, dir: &str, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(args)
            .current_dir(self.work().join(dir))
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.path().join("home"))
            .env("MRSM_ROOT_DIR", self.path().join("host"))
            .env("MRSM_PLUGINS_DIR", self.path().join("host-plugins"))
            .env("MRSM_PYTHON", self.path().join("no-python"))
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"system":{"cli":{"check_updates":false}}}"#,
            )
            .output()
            .unwrap()
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_in("", args)
    }

    fn ok_in(&self, dir: &str, args: &[&str]) -> String {
        let output = self.run_in(dir, args);
        let text = text(&output);
        assert!(output.status.success(), "{args:?} in '{dir}':\n{text}");
        text
    }

    fn jobs(&self) -> JobsContext {
        JobsContext::for_root(self.work().join("root"))
    }

    fn job_names(&self) -> Vec<String> {
        let mut names: Vec<String> = meerschaum_jobs::executor::get_jobs(&self.jobs(), true)
            .into_keys()
            .collect();
        names.sort();
        names
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        for (_, mut job) in meerschaum_jobs::executor::get_jobs(&self.jobs(), true) {
            let _ = job.delete();
        }
    }
}

/// The output as one line of words: messages wrap at the console width.
fn text(output: &Output) -> String {
    strip_ansi(&format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn words(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

const ROOT: &str = "project_name: top\nroot_dir: ./root\nplugins_dir: [./plugins]\ninclude:\n  - projects/\nconfig:\n  meerschaum:\n    instance: sql:local\n  system:\n    cli:\n      check_updates: false\njobs:\n  top-job: show version\n";

fn tree() -> Tree {
    Tree::new(&[
        ("mrsm-compose.yaml", ROOT),
        (
            "projects/a.yaml",
            "project_name: a\nroot_dir: ../root\njobs:\n  a-job: show version\n",
        ),
        (
            "projects/b/mrsm-compose.yaml",
            "project_name: b\nparent: ../..\njobs:\n  b-job: show version\n",
        ),
    ])
}

#[test]
fn a_tree_starts_stamps_and_removes_jobs_per_project() {
    let tree = tree();
    let up = tree.ok_in("", &["compose", "up"]);
    assert!(up.contains("a: Running 1 background job"), "{up}");
    assert_eq!(tree.job_names(), vec!["a-job", "b-job", "top-job"]);
    let root_file = std::fs::canonicalize(tree.work().join("mrsm-compose.yaml")).unwrap();
    for (name, project) in [("a-job", "a"), ("b-job", "b"), ("top-job", "top")] {
        let mut job = Job::open(name, &tree.jobs()).unwrap();
        let env = job.env();
        assert_eq!(
            env.get("MRSM__COMPOSE_PROJECT").map(String::as_str),
            Some(project),
            "{name}"
        );
        assert_eq!(
            env.get("MRSM__COMPOSE_ROOT").map(PathBuf::from).as_deref(),
            Some(root_file.as_path()),
            "{name}"
        );
    }
    // The root's `ps` names each project's jobs.
    let ps = tree.ok_in("", &["compose", "ps", "--nopretty"]);
    for name in ["a-job", "b-job", "top-job"] {
        assert!(ps.contains(name), "{ps}");
    }

    // A file with a `parent:` runs from the parent's tree, touching only its project.
    tree.ok_in("projects/b", &["compose", "down"]);
    assert_eq!(tree.job_names(), vec!["a-job", "top-job"]);

    // An included project without a `parent:` leaves the root's jobs alone.
    let alone = tree.run(&["compose", "down", "--file", "projects/a.yaml"]);
    let alone_text = text(&alone);
    assert!(alone.status.success(), "{alone_text}");
    assert!(
        words(&alone_text).contains("managed by the root compose at")
            && words(&alone_text).contains("rerun from there or pass `--force`"),
        "{alone_text}"
    );
    assert_eq!(tree.job_names(), vec!["a-job", "top-job"]);

    // `--project` limits the root's operation to one project.
    tree.ok_in("", &["compose", "down", "--project", "a"]);
    assert_eq!(tree.job_names(), vec!["top-job"]);
    tree.ok_in("", &["compose", "down"]);
    assert!(tree.job_names().is_empty());
}

#[test]
fn explain_spans_the_tree_and_changes_nothing() {
    let tree = tree();
    let out = tree.ok_in("", &["compose", "explain"]);
    for name in ["Pipes in 'a'", "Pipes in 'b'", "Pipes in 'top'"] {
        assert!(out.contains(name), "{out}");
    }
    // Side-effect free: no root, no cache, no config directory.
    assert!(!tree.work().join("root").exists());
    let mut entries: Vec<String> = std::fs::read_dir(tree.work())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    entries.sort();
    assert_eq!(entries, vec!["mrsm-compose.yaml", "projects"]);
    // From the file with a `parent:`: only that project.
    let out = tree.ok_in("projects/b", &["compose", "explain"]);
    assert!(
        out.contains("Pipes in 'b'") && !out.contains("Pipes in 'a'"),
        "{out}"
    );
}

#[test]
fn explain_writes_nothing_under_a_fresh_root() {
    // The generic start-up creates `config/` and `cache/` for most commands; `explain` is
    // side-effect free end to end (decision 12).
    let tree = tree();
    let out = tree.ok_in("", &["compose", "explain"]);
    assert!(out.contains("Pipes in 'top'"), "{out}");
    let host = tree.path().join("host");
    let left: Vec<String> = walk(&host);
    assert!(left.is_empty(), "explain left files in the root: {left:?}");
    // A command that does start the instance still initializes the root.
    tree.ok_in("", &["show", "version"]);
    assert!(!walk(&host).is_empty());
}

/// Every file and directory under `dir`, relative.
fn walk(dir: &Path) -> Vec<String> {
    fn visit(base: &Path, dir: &Path, out: &mut Vec<String>) {
        let Ok(read) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in read.filter_map(Result::ok) {
            let path = entry.path();
            out.push(path.strip_prefix(base).unwrap().display().to_string());
            if path.is_dir() {
                visit(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    visit(dir, dir, &mut out);
    out.sort();
    out
}

#[test]
fn compose_never_writes_screen_clear_codes_to_a_pipe() {
    let tree = tree();
    tree.ok_in("", &["compose", "up"]);
    let down = tree.run(&["compose", "down"]);
    let raw = format!(
        "{}{}",
        String::from_utf8_lossy(&down.stdout),
        String::from_utf8_lossy(&down.stderr)
    );
    assert!(down.status.success(), "{raw}");
    assert!(
        !raw.contains("\u{1b}[2J") && !raw.contains("\u{1b}[H"),
        "{raw:?}"
    );
}

#[test]
fn the_file_flag_works_before_and_after_the_subaction() {
    let tree = Tree::new(&[
        (
            "one.yaml",
            "project_name: one\nroot_dir: ./root\nconfig:\n  meerschaum:\n    instance: sql:local\n",
        ),
        (
            "two.yaml",
            "project_name: two\nroot_dir: ./root\nconfig:\n  meerschaum:\n    instance: sql:local\n",
        ),
    ]);
    for args in [
        vec!["compose", "--file", "two.yaml", "explain"],
        vec!["compose", "explain", "--file", "two.yaml"],
        vec!["compose", "--compose-file", "two.yaml", "explain"],
        vec!["compose", "explain", "-f", "two.yaml"],
        vec!["compose", "-f", "two.yaml", "explain"],
        vec!["compose", "--file=two.yaml", "explain"],
    ] {
        let out = tree.run(&args);
        let out = text(&out);
        assert!(out.contains("Pipes in 'two'"), "{args:?}:\n{out}");
    }
}

#[test]
fn missing_references_are_findings_in_explain_and_errors_in_up() {
    let tree = Tree::new(&[(
        "mrsm-compose.yaml",
        "project_name: refs\nroot_dir: ./root\nconnectors:\n  mqtt:\n    x: '{mqtt:nope}'\npipes:\n  - connector: plugin:absent\n    metric: m\n    instance: sql:local\nconfig:\n  meerschaum:\n    instance: sql:local\n",
    )]);
    let explain = tree.run(&["compose", "explain"]);
    let out = text(&explain);
    assert!(explain.status.success(), "{out}");
    assert!(out.contains("Pipes in 'refs'"), "{out}");
    let flat = words(&out);
    assert!(
        flat.contains("connector 'mqtt:nope' is not defined on this host")
            && flat.contains("config.meerschaum.connectors.mqtt.x"),
        "{out}"
    );
    assert!(flat.contains("plugin 'absent'"), "{out}");
    assert!(!tree.work().join("root").exists());

    let up = tree.run(&["compose", "up"]);
    let out = text(&up);
    assert!(!up.status.success(), "{out}");
    assert!(
        words(&out).contains(
            "connector 'mqtt:nope' is not defined on this host (referenced by config.meerschaum.connectors.mqtt.x)"
        ),
        "{out}"
    );
    assert!(!out.contains("TypeError"), "{out}");
    assert!(
        !tree.work().join("root").exists(),
        "nothing was initialized"
    );
}

#[test]
fn every_compose_error_exits_non_zero() {
    let tree = Tree::new(&[(
        "mrsm-compose.yaml",
        "project_name: p\nroot_dir: ./root\ninclude: [missing.yaml]\n",
    )]);
    for args in [
        vec!["compose", "explain"],
        vec!["compose", "up"],
        vec!["compose", "down"],
        vec!["compose", "--file", "nope.yaml", "explain"],
        vec!["compose", "explain", "--project", "nope"],
    ] {
        let out = tree.run(&args);
        assert!(!out.status.success(), "{args:?}: {}", text(&out));
    }
}

#[test]
fn explain_renders_v4s_table_in_color_and_plain() {
    use meerschaum_cli::compose_action::render_explain;
    use meerschaum_cli::format::{FormatSettings, Formatter};
    use meerschaum_compose::command::RecordingRunner;
    use meerschaum_compose::ops::{InMemoryInstance, InMemoryJobs, NoInput};
    use meerschaum_compose::project::{Host, Session};
    use meerschaum_compose::subactions::{ComposeArgs, Services, compose};
    use meerschaum_core::config::{Config, ConfigOptions};

    let tree = Tree::new(&[(
        "mrsm-compose.yaml",
        "project_name: shown\nroot_dir: ./root\nconfig:\n  meerschaum:\n    instance: sql:local\npipes:\n  - connector: sql:local\n    metric: temp\n    location: atlanta\n    parameters:\n      columns:\n        datetime: ts\n  - connector: sql:other\n    metric: rain\n    instance: sql:local\n",
    )]);
    let mut env = indexmap::IndexMap::new();
    env.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
    env.insert(
        "HOME".to_string(),
        tree.path().join("home").to_string_lossy().to_string(),
    );
    env.insert(
        "MRSM_ROOT_DIR".to_string(),
        tree.path().join("host").to_string_lossy().to_string(),
    );
    env.insert(
        "MRSM_PLUGINS_DIR".to_string(),
        tree.path()
            .join("host-plugins")
            .to_string_lossy()
            .to_string(),
    );
    let host = Host::new(tree.work(), env).unwrap();
    let args = ComposeArgs {
        action: vec!["explain".into()],
        sysargs: vec!["compose".into(), "explain".into()],
        width: Some(100),
        ..ComposeArgs::default()
    };
    let out = compose(
        &host,
        &mut Session::default(),
        &args,
        Services {
            runner: &mut RecordingRunner::default(),
            instance: &mut InMemoryInstance::default(),
            jobs: &mut InMemoryJobs::default(),
            prompter: &mut NoInput,
        },
    );
    assert!(out.result.as_ref().unwrap().success);
    let model = out.explain_models.first().expect("the table as data");
    let plain_reference = out.explain.clone().unwrap();

    let mut opts = ConfigOptions::with_env(vec![
        (
            "MRSM_ROOT_DIR".into(),
            tree.path().join("host").to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".into(),
            tree.path()
                .join("host-plugins")
                .to_string_lossy()
                .into_owned(),
        ),
    ]);
    opts.allow_writes = false;
    opts.home = tree.path().join("home");
    let config = Config::new(opts).unwrap();
    let mut settings = FormatSettings::detect(&config, |_| None);
    settings.width = 100;
    settings.ansi = true;
    settings.no_color = false;
    let colored = render_explain(&Formatter::new(settings.clone(), &config), model);
    assert!(colored.contains("\u{1b}["), "the table has v4's colors");
    // Pipe keys, panels and JSON are styled, and the layout is the plain table's.
    assert_eq!(strip_ansi(&colored), plain_reference);

    let mut plain_settings = settings;
    plain_settings.no_color = true;
    let plain = render_explain(&Formatter::new(plain_settings, &config), model);
    assert_eq!(strip_ansi(&plain), plain_reference);
}
