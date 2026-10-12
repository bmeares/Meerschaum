//! The native actions that need no instance: `show *`, `edit config`, `reload`, `python`,
//! `compose`, and the dispatcher's failure mapping. The output formatting itself is covered by
//! `tests/format.rs`; these tests pin the action logic.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use meerschaum_cli::dispatch::{self, CommandSpec, Host, RecordingHost, Runtime};
use meerschaum_cli::format::FormatSettings;
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};

#[derive(Clone, Default)]
struct SharedHost(Rc<RefCell<RecordingHost>>);

impl Host for SharedHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        self.0.borrow_mut().run(spec)
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        self.0.borrow_mut().read_line(prompt)
    }
}

fn config(root: &Path, writes: bool) -> Config {
    let mut opts = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = writes;
    opts.home = root.to_path_buf();
    Config::new(opts).unwrap()
}

fn run(
    root: &Path,
    sysargs: &[&str],
    env: fn(&str) -> Option<String>,
    host: &SharedHost,
) -> (SuccessTuple, String) {
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, env);
        rt.config = Some(config(root, true));
        rt.format = Some(FormatSettings {
            ansi: false,
            unicode: true,
            width: 80,
            cols: 80,
            no_color: false,
        });
        rt.host = Box::new(host.clone());
        rt.cwd = Some(root.to_path_buf());
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    (result, String::from_utf8(out).unwrap())
}

fn no_env(_: &str) -> Option<String> {
    None
}

#[test]
fn show_version_and_config() {
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    let (r, out) = run(tmp.path(), &["show", "version"], no_env, &host);
    assert_eq!(r, SuccessTuple::success());
    assert_eq!(
        out,
        format!(" 💬 Meerschaum v{}\n", meerschaum_cli::VERSION)
    );
    let (_, out) = run(
        tmp.path(),
        &["show", "version", "--nopretty"],
        no_env,
        &host,
    );
    assert_eq!(out, format!("{}\n", meerschaum_cli::VERSION));

    let (r, out) = run(
        tmp.path(),
        &["show", "config", "meerschaum", "--nopretty"],
        no_env,
        &host,
    );
    assert!(r.success);
    assert!(out.starts_with("{\"instance\": \"sql:main\""), "{out}");
    // Passwords are only masked when pretty-printing.
    let (_, out) = run(tmp.path(), &["show", "config", "meerschaum"], no_env, &host);
    assert!(out.contains("'password': '****'"), "{out}");

    let (r, _) = run(
        tmp.path(),
        &["show", "config", "meerschaum:instance"],
        no_env,
        &host,
    );
    assert_eq!(
        r,
        SuccessTuple::fail("Invalid configuration keys '['meerschaum:instance']'.")
    );
    let (r, _) = run(
        tmp.path(),
        &["show", "config", "meerschaum", "instance"],
        no_env,
        &host,
    );
    assert_eq!(
        r,
        SuccessTuple::fail(
            "Failed to execute `show config meerschaum instance` with `AttributeError`:\n\n'str' object has no attribute 'items'"
        )
    );
    let (r, out) = run(tmp.path(), &["show", "config"], no_env, &host);
    assert!(r.success);
    for key in ["'meerschaum'", "'system'", "'formatting'", "'shell'"] {
        assert!(out.contains(key), "{key} missing from `show config`");
    }
}

#[test]
fn show_environment_lists_mrsm_variables() {
    fn env(k: &str) -> Option<String> {
        (k == "MRSM_TEST").then(|| "1".into())
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut out = Vec::new();
    {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, env);
        rt.config = Some(config(tmp.path(), false));
        rt.format = Some(FormatSettings {
            ansi: false,
            unicode: true,
            width: 80,
            cols: 80,
            no_color: false,
        });
        rt.vars = || {
            vec![
                ("HOME".into(), "/home/x".into()),
                ("MRSM_ROOT_DIR".into(), "/r".into()),
                ("MRSM_SQL_DB_PASSWORD".into(), "pw".into()),
            ]
        };
        dispatch::entry(&["show".into(), "environment".into()], &mut rt);
    }
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "{'MRSM_ROOT_DIR': '/r', 'MRSM_SQL_DB_PASSWORD': '**'}\n"
    );
}

#[test]
fn show_plugins_scans_the_plugins_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let plugins = tmp.path().join("plugins");
    std::fs::create_dir_all(plugins.join("pkg")).unwrap();
    std::fs::write(plugins.join("pkg").join("__init__.py"), "").unwrap();
    std::fs::create_dir_all(plugins.join("not_a_plugin")).unwrap();
    std::fs::write(plugins.join("single.py"), "").unwrap();
    std::fs::write(plugins.join("README.md"), "").unwrap();
    assert_eq!(
        meerschaum_cli::builtin::plugin_names(&[plugins]),
        vec!["pkg".to_string(), "single".to_string()]
    );
    let host = SharedHost::default();
    let (r, out) = run(
        tmp.path(),
        &["show", "plugins", "--nopretty"],
        no_env,
        &host,
    );
    assert!(r.success);
    assert_eq!(out, "pkg\nsingle\n");
}

#[test]
fn edit_config_opens_the_editor_then_reloads() {
    fn env(k: &str) -> Option<String> {
        (k == "EDITOR").then(|| "my-editor".into())
    }
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    let (r, out) = run(tmp.path(), &["edit", "config", "system"], env, &host);
    assert_eq!(r, SuccessTuple::success());
    let commands = host.0.borrow().commands.clone();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].program, "my-editor");
    let path = std::path::PathBuf::from(&commands[0].args[0]);
    assert_eq!(path.file_name().unwrap(), "system.json");
    assert!(path.exists(), "the key's file is created before editing");
    assert!(out.contains("Nothing to reload"), "{out}");
}

#[test]
fn python_writes_an_init_script_and_starts_the_interpreter() {
    fn env(k: &str) -> Option<String> {
        (k == "MRSM_PYTHON").then(|| "/opt/py/bin/python".into())
    }
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    let (r, _) = run(tmp.path(), &["python", "print(1)", "--noask"], env, &host);
    assert_eq!(r, SuccessTuple::success());
    let commands = host.0.borrow().commands.clone();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].program, "/opt/py/bin/python");
    assert_eq!(
        commands[0].args.len(),
        1,
        "--noask runs the script without -i"
    );
    assert!(commands[0].args[0].ends_with(".py"));
    assert!(
        !Path::new(&commands[0].args[0]).exists(),
        "the script is removed"
    );

    host.0.borrow_mut().commands.clear();
    host.0.borrow_mut().exit_code = 3;
    let (r, _) = run(tmp.path(), &["python", "-A", "-V"], env, &host);
    assert_eq!(r, SuccessTuple::fail("Python interpreter returned 3."));
    let commands = host.0.borrow().commands.clone();
    assert_eq!(commands[0].args, vec!["-V".to_string()]);

    assert_eq!(
        meerschaum_cli::builtin::python_init_script(
            &["x".into(), "=".into(), "1;".into(), "print(x)".into()],
            None,
            &[],
            true
        ),
        "import meerschaum as mrsm; x = 1; print(x); "
    );
}

#[test]
fn compose_explain_and_unsupported_subactions() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("proj");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(
        project.join("mrsm-compose.yaml"),
        "project_name: demo\nsync:\n  pipes:\n    - connector: plugin:example\n      metric: temp\n",
    )
    .unwrap();
    let host = SharedHost::default();
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, no_env);
        rt.config = Some(config(tmp.path(), true));
        rt.format = Some(FormatSettings {
            ansi: false,
            unicode: true,
            width: 100,
            cols: 100,
            no_color: false,
        });
        rt.host = Box::new(host.clone());
        rt.cwd = Some(project.clone());
        rt.vars = || vec![("PATH".into(), "/usr/bin:/bin".into())];
        dispatch::entry(&["compose".into(), "explain".into()], &mut rt)
    };
    assert!(result.success, "{result:?}");
    let out = String::from_utf8(out).unwrap();
    assert!(out.contains("Pipes in 'demo'"), "{out}");
    // `init_root` re-invokes `mrsm show version --no-daemon` with captured output.
    let commands = host.0.borrow().commands.clone();
    assert!(
        commands.iter().any(|c| c.args.starts_with(&[
            "show".into(),
            "version".into(),
            "--no-daemon".into()
        ]) && c.quiet),
        "{commands:?}"
    );

    let (r, _) = run(tmp.path(), &["compose", "down"], no_env, &host);
    assert_eq!(
        r,
        SuccessTuple::fail("action `compose down` is not implemented in Rust yet")
    );
}

#[test]
fn other_actions_are_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    let (r, _) = run(tmp.path(), &["sync", "pipes"], no_env, &host);
    assert_eq!(
        r,
        SuccessTuple::fail("action `sync pipes` is not implemented in Rust yet")
    );
    let (r, _) = run(tmp.path(), &["reload"], no_env, &host);
    assert_eq!(r, SuccessTuple::success());
}
