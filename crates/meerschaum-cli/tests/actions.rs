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

/// Point the Python workers these tests spawn at this repository. A worker inherits the test
/// process's environment, and without `PYTHONPATH` the interpreter imports a stale installed
/// Meerschaum (`~/.local`) that has no `meerschaum.worker` (and prints a traceback to stderr).
fn use_repo_python() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo = repo.canonicalize().unwrap_or(repo);
        let mut paths = vec![repo];
        if let Some(existing) = std::env::var_os("PYTHONPATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        if let Ok(joined) = std::env::join_paths(paths) {
            // SAFETY: the process environment is only written here, once, before any test of
            // this binary spawns a worker (every test reaches it through `run`).
            unsafe { std::env::set_var("PYTHONPATH", joined) };
        }
    });
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
    use_repo_python();
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
fn invalid_duration_arguments_fail_before_an_action_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    for (option, value, expected) in [
        ("--min-seconds", "inf", "--min-seconds"),
        ("--cooldown", "NaN", "--min-seconds"),
        ("--min-seconds", "1e300", "--min-seconds"),
        ("--timeout", "inf", "--timeout-seconds"),
        ("--timeout-seconds", "NaN", "--timeout-seconds"),
        ("--timeout-seconds", "1e300", "--timeout-seconds"),
    ] {
        let (result, output) = run(
            tmp.path(),
            &["restart", "jobs", "--loop", option, value],
            no_env,
            &host,
        );
        assert!(!result.success, "{option} {value}: {result:?}");
        assert!(result.message.contains(expected), "{}", result.message);
        assert!(result.message.contains("finite"), "{}", result.message);
        assert!(!output.contains("Checking jobs"), "{output}");
    }
    // Normal durations still pass, including the existing negative-value clamping behavior.
    for value in ["0", "0.25", "-1"] {
        let (result, _) = run(
            tmp.path(),
            &["show", "version", "--min-seconds", value],
            no_env,
            &host,
        );
        assert!(result.success, "{value}: {result:?}");
    }
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
fn pipelines_dispatch_core_steps_natively() {
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    let (result, out) = run(
        tmp.path(),
        &["show", "version", "+", "show", "version", ":", "--nopretty"],
        no_env,
        &host,
    );
    assert!(result.success, "{result:?}");
    assert_eq!(out.matches(meerschaum_cli::VERSION).count(), 2, "{out}");
    let params = r#"{"sub_args_line":"show version","patch_args":{"nopretty":true}}"#;
    let (result, out) = run(
        tmp.path(),
        &[
            "start",
            "pipeline",
            "x3",
            "--min-seconds",
            "0",
            "-P",
            params,
        ],
        no_env,
        &host,
    );
    assert!(result.success, "{result:?}");
    assert!(
        out.starts_with(&format!("{}\n", meerschaum_cli::VERSION)),
        "{out}"
    );
    assert_eq!(out.matches(meerschaum_cli::VERSION).count(), 3, "{out}");
    assert!(out.contains("Ran the action chain 3 times."), "{out}");
    // The current form of a serialized chain runs the same way.
    let (result, out) = run(
        tmp.path(),
        &["chain", "x2", "--min-seconds", "0", "-P", params],
        no_env,
        &host,
    );
    assert!(result.success, "{result:?}");
    assert_eq!(out.matches(meerschaum_cli::VERSION).count(), 2, "{out}");
    assert!(out.contains("Ran the action chain 2 times."), "{out}");
}

#[test]
fn sql_metadata_workers_preserve_exact_action_output() {
    use meerschaum_connector_sql::{
        SqlConnector,
        flavor::Flavor,
        uri::{ConnectionSpec, ConnectorAttributes},
    };
    use meerschaum_core::{Pipe, Value};

    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("sqlite")).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            tmp.path()
                .join("sqlite/mrsm_local.db")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    let connector = runtime
        .block_on(SqlConnector::connect(
            "local",
            ConnectionSpec::from_attributes(&attrs).unwrap(),
        ))
        .unwrap();
    for metric in ["populated", "empty", "missing"] {
        let mut pipe = Pipe::builder("sql:local", metric)
            .instance("sql:local")
            .build()
            .unwrap();
        pipe.set_parameters(
            Value::from_json_str(
                &serde_json::json!({
                    "target": format!("metadata_{metric}"),
                    "columns": {"id": "id"},
                })
                .to_string(),
            )
            .unwrap()
            .to_map(),
        );
        assert!(runtime.block_on(connector.register_pipe(&pipe)).success);
    }
    let queries = [
        "CREATE TABLE metadata_populated (id INTEGER)".into(),
        "INSERT INTO metadata_populated VALUES (1), (2)".into(),
        "CREATE TABLE metadata_empty (id INTEGER)".into(),
    ];
    assert!(
        runtime
            .block_on(connector.exec_queries(&queries, true, true))
            .iter()
            .all(Option::is_some)
    );
    let host = SharedHost::default();
    for action in ["partitions", "rowcounts", "sizes"] {
        for nopretty in [false, true] {
            let mut args = vec!["show", action, "-i", "sql:local", "--workers", "1"];
            if nopretty {
                args.push("--nopretty");
            }
            let serial = run(tmp.path(), &args, no_env, &host);
            args[5] = "4";
            let parallel = run(tmp.path(), &args, no_env, &host);
            assert!(serial.0.success, "{action}: {:?}", serial.0);
            assert_eq!(parallel, serial, "{action}, nopretty={nopretty}");
            assert!(parallel.1.contains("populated"), "{}", parallel.1);
            assert!(parallel.1.contains("empty"), "{}", parallel.1);
            assert!(parallel.1.contains("missing"), "{}", parallel.1);
        }
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
    // Startup discovers plugin metadata. Reuse the reference interpreter's environment
    // rather than asking Python to create dependency environments for these empty modules.
    #[cfg(unix)]
    {
        let reference = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.venv-ref");
        if reference.exists() {
            let venvs = tmp.path().join("venvs");
            std::fs::create_dir_all(&venvs).unwrap();
            for name in ["mrsm", "pkg", "single", "not_a_plugin"] {
                std::os::unix::fs::symlink(reference.canonicalize().unwrap(), venvs.join(name))
                    .unwrap();
            }
        }
    }
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
    assert!(!out.contains("Nothing to reload"), "{out}");
}

#[test]
fn edit_config_keeps_yaml_and_accepts_its_filename() {
    fn env(k: &str) -> Option<String> {
        (k == "EDITOR").then(|| "my-editor".into())
    }
    for key in ["stack", "stack.yaml"] {
        let tmp = tempfile::tempdir().unwrap();
        let config_dir = tmp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let path = config_dir.join("stack.yaml");
        let original = "project_name: example\n";
        std::fs::write(&path, original).unwrap();
        let host = SharedHost::default();
        let (r, out) = run(tmp.path(), &["edit", "config", key], env, &host);
        assert_eq!(r, SuccessTuple::success(), "{out}");
        assert_eq!(
            host.0.borrow().commands[0].args.last().unwrap().as_str(),
            path.to_str().unwrap()
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
        assert!(!config_dir.join("stack.yaml.json").exists());
        assert!(!config_dir.join("stack.json").exists());
    }
}

/// v4 pops the key from the in-memory config before `get_config(key, write_missing=True)`, so a
/// key an `MRSM_CONFIG` patch already put in memory still gets its file created from the
/// defaults (and the edit does not report an unparseable, empty file).
#[test]
fn edit_config_creates_the_key_file_when_mrsm_config_patched_the_key() {
    fn env(k: &str) -> Option<String> {
        (k == "EDITOR").then(|| "my-editor".into())
    }
    use_repo_python();
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    let mut opts = ConfigOptions::with_env(vec![
        (
            "MRSM_ROOT_DIR".into(),
            tmp.path().to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".into(),
            tmp.path().join("plugins").to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"meerschaum": {"instance": "sql:local"}}"#.into(),
        ),
    ]);
    opts.allow_writes = true;
    opts.home = tmp.path().to_path_buf();
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, env);
        rt.config = Some(Config::new(opts).unwrap());
        rt.host = Box::new(host.clone());
        rt.cwd = Some(tmp.path().to_path_buf());
        let sysargs: Vec<String> = ["edit", "config", "system"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    let out = String::from_utf8(out).unwrap();
    assert_eq!(result, SuccessTuple::success(), "{out}");
    assert!(!out.contains("Could not parse"), "{out}");
    let commands = host.0.borrow().commands.clone();
    assert_eq!(commands.len(), 1, "one editor session, no re-edit loop");
    let path = std::path::PathBuf::from(&commands[0].args[0]);
    assert!(path.exists(), "the key's file is created before editing");
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.trim().is_empty(), "the file holds the key's config");
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
    assert_eq!(commands.len(), 2, "the import probe, then the interpreter");
    assert_eq!(commands[0].program, "/opt/py/bin/python");
    assert_eq!(
        commands[0].args,
        vec!["-c".to_string(), "import meerschaum".to_string()],
        "the chosen interpreter is asked first"
    );
    let commands = &commands[1..];
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

    // An interpreter without the package: a failure naming the install, no REPL.
    host.0.borrow_mut().commands.clear();
    host.0.borrow_mut().exit_code = 1;
    let (r, _) = run(tmp.path(), &["python", "print(1)", "--noask"], env, &host);
    assert!(!r.success);
    assert_eq!(
        r.message,
        format!(
            "The meerschaum Python package is not installed for /opt/py/bin/python; install it \
             with pip install meerschaum=={} to use mrsm python",
            meerschaum_core::python_guard::expected_version()
        )
    );
    assert_eq!(
        host.0.borrow().commands.len(),
        1,
        "nothing but the probe ran"
    );
    host.0.borrow_mut().exit_code = 0;

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
fn compose_explain_and_native_down() {
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
    // v5: `explain` is side-effect free; it does not re-invoke `mrsm show version` to
    // initialize the root (v4 did).
    let commands = host.0.borrow().commands.clone();
    assert!(commands.is_empty(), "{commands:?}");

    let plugins = tmp.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(["compose", "down"])
        .current_dir(&project)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", tmp.path())
        .env("MRSM_ROOT_DIR", tmp.path())
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_PYTHON", tmp.path().join("absent-python"))
        .env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:local"}}"#)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{text}");
    assert!(
        !text.contains("failed to start the Python worker"),
        "{text}"
    );
}

#[test]
fn python_only_actions_dispatch_to_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let host = SharedHost::default();
    // `install plugins`, `show packages` and the rest of the registry are native now; checking
    // that a Python plugin imports still needs the worker.
    std::fs::create_dir_all(tmp.path().join("plugins")).unwrap();
    std::fs::write(
        tmp.path().join("plugins/synthetic_wiring_test.py"),
        "x = 1\n",
    )
    .unwrap();
    assert_worker_dispatch(tmp.path(), &["verify", "plugins"]);
    // The worker imports `meerschaum`, which needs the built `_core` extension (D14). The Rust CI
    // job does not build it and says so with `MRSM_TEST_NO_CORE=1`; pytest runs `mrsm reload`
    // with the extension (`tests/test_actions_facade.py`). Anywhere else a missing or stale
    // extension fails here.
    if std::env::var_os("MRSM_TEST_NO_CORE").is_some() {
        eprintln!("skipped `reload`: MRSM_TEST_NO_CORE is set");
        return;
    }
    let (r, _) = run(tmp.path(), &["reload"], no_env, &host);
    assert_eq!(r, SuccessTuple::success());
}

/// A deliberately absent interpreter proves delegation without installing anything or
/// starting Docker. Process-local environment also keeps concurrent tests isolated.
fn assert_worker_dispatch(root: &Path, args: &[&str]) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(args)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", root.join("plugins"))
        .env("MRSM_PYTHON", root.join("absent-python"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!text.contains("is not implemented in Rust yet"), "{text}");
    // The worker crate refuses the interpreter with one message (W18-FX2); the CLI prints it
    // once, not once per layer (manifest warning, exception line, result).
    assert_eq!(text.matches("could not be started").count(), 1, "{text}");
    assert!(text.contains("WorkerError"), "{text}");
}
