//! `compose up` registers each new pipe with an in-process `register pipes` command, like v4's
//! `run_mrsm_command(..., _subprocess=False)` (`entry(sysargs)`): the command's result is returned,
//! never printed, and every registration shares one Python worker. Before, each registration was a
//! child `mrsm` that printed "Successfully registered 1 pipe." and started its own worker for the
//! connector's `register()` hook, so a large compose file took several times v4's time.
//!
//! The project is synthetic: generated SQL pipes, pipes of an unknown connector type (v4's
//! `Pipe.connector` falls back to the keys and prints nothing), and `plugin:` pipes with and
//! without `register()`. `MRSM_PYTHON` is a wrapper that counts the Python processes started.
#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const SQL_PIPES: usize = 100;
const UNKNOWN_PIPES: usize = 5;
const HOOK_PIPES: usize = 3;
const PLAIN_PIPES: usize = 3;

struct Project {
    temp: tempfile::TempDir,
}

impl Project {
    fn new() -> Project {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path();
        for dir in ["host", "host-plugins", "home", "plugins"] {
            std::fs::create_dir(path.join(dir)).unwrap();
        }
        std::fs::write(
            path.join("plugins/w17cf2hook.py"),
            "required = []\n\ndef register(pipe, **kw):\n    return {'columns': {'datetime': 'dt'}, 'from_hook': True}\n\n\ndef fetch(pipe, **kw):\n    return None\n",
        )
        .unwrap();
        std::fs::write(
            path.join("plugins/w17cf2plain.py"),
            "required = []\n\ndef fetch(pipe, **kw):\n    return None\n",
        )
        .unwrap();
        let mut body = String::from(
            "project_name: w17cf2reg\nroot_dir: ./root\nplugins_dir: [./plugins]\nsync:\n  pipes:\n",
        );
        for i in 0..SQL_PIPES {
            body.push_str(&format!(
                "    - connector: sql:w17cf2src\n      metric: m{i}\n      parameters:\n        sql: SELECT {i} AS id\n        columns: {{id: id}}\n"
            ));
        }
        for i in 0..UNKNOWN_PIPES {
            body.push_str(&format!(
                "    - connector: w17cf2unknown\n      metric: u{i}\n"
            ));
        }
        for i in 0..HOOK_PIPES {
            body.push_str(&format!(
                "    - connector: plugin:w17cf2hook\n      metric: h{i}\n"
            ));
        }
        for i in 0..PLAIN_PIPES {
            body.push_str(&format!(
                "    - connector: plugin:w17cf2plain\n      metric: p{i}\n"
            ));
        }
        body.push_str(
            "config:\n  meerschaum:\n    instance: sql:w17cf2inst\n    connectors:\n      sql:\n        w17cf2inst: {flavor: sqlite, database: ./inst.db}\n        w17cf2src: {flavor: sqlite, database: ./src.db}\n  system:\n    cli:\n      check_updates: false\n",
        );
        std::fs::write(path.join("mrsm-compose.yaml"), body).unwrap();
        // Count every Python process (the plugin manifest and the workers).
        let wrapper = path.join("python-wrapper");
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nprintf '%s %s\\n' \"$1\" \"$2 $(printf %s \"$3\" | head -c 60 | tr '\\n' ' ')\" >> '{}'\nexec '{}' \"$@\"\n",
                path.join("python-starts").display(),
                python().display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        Project { temp }
    }

    fn path(&self) -> &Path {
        self.temp.path()
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with(args, &[])
    }

    fn run_with(&self, args: &[&str], extra: &[(&str, String)]) -> Output {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo = repo.canonicalize().unwrap_or(repo);
        Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(args)
            .current_dir(self.path())
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.path().join("home"))
            .env("PYTHONPATH", repo)
            .env("MRSM_ROOT_DIR", self.path().join("host"))
            .env("MRSM_PLUGINS_DIR", self.path().join("host-plugins"))
            .env("MRSM_PYTHON", self.path().join("python-wrapper"))
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:w17cf2inst"},"system":{"cli":{"check_updates":false}}}"#,
            )
            .envs(extra.iter().map(|(k, v)| (*k, v.as_str())))
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn python_starts(&self) -> usize {
        std::fs::read_to_string(self.path().join("python-starts"))
            .map(|text| text.lines().count())
            .unwrap_or(0)
    }
}

/// Stdout and stderr without ANSI escape sequences.
fn text(output: &Output) -> (String, String) {
    fn strip(bytes: &[u8]) -> String {
        let text = String::from_utf8_lossy(bytes);
        let mut out = String::with_capacity(text.len());
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
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
    (strip(&output.stdout), strip(&output.stderr))
}

/// The interpreter the wrapper runs: the caller's `MRSM_PYTHON` (`${MRSM_PYTHON:-python3}`).
fn python() -> std::path::PathBuf {
    std::env::var_os("MRSM_PYTHON")
        .filter(|p| !p.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "python3".into())
}

fn python3_available() -> bool {
    Command::new(python())
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[test]
fn compose_up_registers_a_large_file_in_process_like_v4() {
    if !python3_available() {
        eprintln!("skipped: no python3 for the plugin worker");
        return;
    }
    let project = Project::new();
    let total = SQL_PIPES + UNKNOWN_PIPES + HOOK_PIPES + PLAIN_PIPES;

    let started = Instant::now();
    let output = project.run(&["compose", "up", "--dry"]);
    let elapsed = started.elapsed();
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}\n{stderr}");

    // v4 prints `Registering Pipe(...)...` per new pipe and nothing of the command's result.
    assert_eq!(
        stdout.matches("Registering Pipe(").count(),
        total,
        "{stdout}\n{stderr}"
    );
    assert!(!stdout.contains("Successfully registered"), "{stdout}");
    // An unknown connector type is silent (v4's `Pipe.connector` swallows the warning).
    assert!(!stderr.contains("Cannot create Connector"), "{stderr}");
    assert!(
        stdout.contains(&format!("Updated parameters for {total} pipes")),
        "{stdout}"
    );

    // A constant number of Python processes (the entry-point probes and the workers of the host
    // and project sessions), not one per registration: a child `mrsm` per pipe started a worker
    // for each of the HOOK_PIPES + UNKNOWN_PIPES + PLAIN_PIPES pipes.
    let starts = project.python_starts();
    assert!(
        (1..=6).contains(&starts),
        "{starts} Python processes for {total} registrations:\n{}",
        std::fs::read_to_string(project.path().join("python-starts")).unwrap_or_default()
    );
    // Generous: v5 takes ~2-3 s in a debug build here; one child per pipe took > 10 s.
    assert!(
        elapsed < Duration::from_secs(90),
        "compose up --dry took {elapsed:?}"
    );

    // Every pipe is registered, and the plugin's `register()` parameters were applied.
    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                project.path().join("root").display().to_string(),
            ),
            (
                "MRSM_PLUGINS_DIR".into(),
                project.path().join("plugins").display().to_string(),
            ),
            (
                "MRSM_SQL_W17CF2INST".into(),
                format!("sqlite:///{}", project.path().join("inst.db").display()),
            ),
        ]))
        .unwrap();
    let session = meerschaum_cli::native::Session::new();
    let instance = session.instance("sql:w17cf2inst", &config).unwrap();
    let sql = instance.sql().unwrap();
    let count = session
        .block_on(sql.driver().value("SELECT COUNT(*) AS n FROM mrsm_pipes"))
        .unwrap();
    assert_eq!(count.unwrap().as_i64(), Some(total as i64));
    let hook = meerschaum_core::Pipe::builder("plugin:w17cf2hook", "h0")
        .instance("sql:w17cf2inst")
        .build()
        .unwrap();
    let attributes = session
        .block_on(instance.conn().get_pipe_attributes(&hook))
        .unwrap();
    let parameters = attributes["parameters"].to_map();
    assert_eq!(
        parameters.get("from_hook"),
        Some(&meerschaum_core::Value::Bool(true)),
        "{parameters:?}"
    );
    session.shutdown();

    // Nothing left to register: the second run prints no registration.
    let output = project.run(&["compose", "up", "--dry"]);
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(!stdout.contains("Registering Pipe("), "{stdout}");
}

#[test]
fn register_pipe_of_an_unknown_connector_type_is_silent() {
    let project = Project::new();
    let output = project.run_with(
        &[
            "register",
            "pipe",
            "-c",
            "w17cf2unknown:bar",
            "-m",
            "bar",
            "-i",
            "sql:w17cf2inst",
        ],
        &[(
            "MRSM_SQL_W17CF2INST",
            format!("sqlite:///{}", project.path().join("inst.db").display()),
        )],
    );
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(
        stdout.contains("Successfully registered 1 pipe."),
        "{stdout}"
    );
    assert!(!stderr.contains("Cannot create Connector"), "{stderr}");
}

/// The in-process `register pipes` runs under the project's environment, as v4's per-project
/// process does: a project `environment:` that defines `MRSM_SQL_<LABEL>` for a label the host
/// environment also defines (pointing elsewhere) wins for the instance the pipe is registered on
/// and for the connector the plugin's `register()` builds.
#[test]
fn compose_registration_uses_the_projects_connector_environment() {
    if !python3_available() {
        eprintln!("skipped: no python3 for the plugin worker");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    for dir in ["host", "host-plugins", "home", "plugins"] {
        std::fs::create_dir(path.join(dir)).unwrap();
    }
    std::fs::write(
        path.join("plugins/w17cf2envhook.py"),
        "required = []\n\ndef register(pipe, **kw):\n    import meerschaum as mrsm\n    conn = mrsm.get_connector('sql', 'w17cf2env')\n    return {'seen_uri': str(conn.URI)}\n\n\ndef fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    let project_db = path.join("project.db");
    let host_db = path.join("host.db");
    std::fs::write(
        path.join("mrsm-compose.yaml"),
        format!(
            "project_name: w17cf2env\nroot_dir: ./root\nplugins_dir: [./plugins]\nenvironment:\n  MRSM_SQL_W17CF2ENV: sqlite:///{}\nsync:\n  pipes:\n    - connector: plugin:w17cf2envhook\n      metric: e\n      instance: sql:w17cf2env\nconfig:\n  meerschaum:\n    instance: sql:w17cf2env\n  system:\n    cli:\n      check_updates: false\n",
            project_db.display()
        ),
    )
    .unwrap();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap_or(repo);
    let output = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(["compose", "up", "--dry"])
        .current_dir(path)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", path.join("home"))
        .env("PYTHONPATH", repo)
        .env("MRSM_ROOT_DIR", path.join("host"))
        .env("MRSM_PLUGINS_DIR", path.join("host-plugins"))
        .env("MRSM_PYTHON", python())
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env(
            "MRSM_SQL_W17CF2ENV",
            format!("sqlite:///{}", host_db.display()),
        )
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("Registering Pipe("), "{stdout}\n{stderr}");
    assert!(
        !host_db.exists(),
        "the host's connector was used:\n{stdout}\n{stderr}"
    );

    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                path.join("root").display().to_string(),
            ),
            (
                "MRSM_SQL_W17CF2ENV".into(),
                format!("sqlite:///{}", project_db.display()),
            ),
        ]))
        .unwrap();
    let session = meerschaum_cli::native::Session::new();
    let instance = session.instance("sql:w17cf2env", &config).unwrap();
    let pipe = meerschaum_core::Pipe::builder("plugin:w17cf2envhook", "e")
        .instance("sql:w17cf2env")
        .build()
        .unwrap();
    let attributes = session
        .block_on(instance.conn().get_pipe_attributes(&pipe))
        .unwrap();
    let parameters = attributes["parameters"].to_map();
    let seen = parameters
        .get("seen_uri")
        .map(|v| v.py_str())
        .unwrap_or_default();
    assert!(
        seen.ends_with(&project_db.display().to_string()),
        "{parameters:?}\n{stdout}\n{stderr}"
    );
    session.shutdown();
}

/// `__mrsm_entry_child__` is honored only when the compose runner's environment marker names the
/// same result path: any other command line that starts with it (a job's, a user's) is not run as
/// an in-process child.
#[test]
fn entry_child_needs_the_runners_marker() {
    let project = Project::new();
    let result = project.path().join("result.json");
    let result_arg = result.display().to_string();
    let args = [
        "__mrsm_entry_child__",
        result_arg.as_str(),
        "show",
        "version",
    ];

    let unmarked = project.run(&args);
    assert!(!result.exists(), "{:?}", text(&unmarked));

    let marked = project.run_with(&args, &[("MRSM__ENTRY_CHILD", result_arg.clone())]);
    let (stdout, stderr) = text(&marked);
    assert!(marked.status.success(), "{stdout}\n{stderr}");
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&result).unwrap()).unwrap();
    assert_eq!(written[0], serde_json::Value::Bool(true), "{written}");
    // The result is written, not printed.
    let message = written[1].as_str().unwrap_or_default().to_string();
    assert!(message.is_empty() || !stdout.contains(&message), "{stdout}");
}

/// A `register` that is not a plain function (a `functools.partial`, a callable object) is still
/// the plugin's hook, as v4's `PluginConnector` calls any `register` attribute: the manifest lists
/// it and its parameters are applied.
#[test]
fn a_non_function_register_hook_is_applied() {
    if !python3_available() {
        eprintln!("skipped: no python3 for the plugin worker");
        return;
    }
    let project = Project::new();
    let plugins = project.path().join("plugins");
    std::fs::write(
        plugins.join("w17cf2partial.py"),
        "import functools\nrequired = []\n\ndef _register(pipe, marker=None, **kw):\n    return {'hook': marker}\n\nregister = functools.partial(_register, marker='partial')\n\n\ndef fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    std::fs::write(
        plugins.join("w17cf2callable.py"),
        "required = []\n\nclass _Register:\n    def __call__(self, pipe, **kw):\n        return {'hook': 'callable'}\n\nregister = _Register()\n\n\ndef fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    let db = project.path().join("hooks.db");
    let uri = format!("sqlite:///{}", db.display());
    for keys in ["plugin:w17cf2partial", "plugin:w17cf2callable"] {
        let output = project.run_with(
            &[
                "register",
                "pipe",
                "-c",
                keys,
                "-m",
                "h",
                "-i",
                "sql:w17cf2hooks",
            ],
            &[
                ("MRSM_PLUGINS_DIR", plugins.display().to_string()),
                ("MRSM_SQL_W17CF2HOOKS", uri.clone()),
            ],
        );
        let (stdout, stderr) = text(&output);
        assert!(output.status.success(), "{stdout}\n{stderr}");
    }
    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                project.path().join("host").display().to_string(),
            ),
            ("MRSM_SQL_W17CF2HOOKS".into(), uri.clone()),
        ]))
        .unwrap();
    let session = meerschaum_cli::native::Session::new();
    let instance = session.instance("sql:w17cf2hooks", &config).unwrap();
    for (keys, expected) in [
        ("plugin:w17cf2partial", "partial"),
        ("plugin:w17cf2callable", "callable"),
    ] {
        let pipe = meerschaum_core::Pipe::builder(keys, "h")
            .instance("sql:w17cf2hooks")
            .build()
            .unwrap();
        let attributes = session
            .block_on(instance.conn().get_pipe_attributes(&pipe))
            .unwrap();
        let parameters = attributes["parameters"].to_map();
        assert_eq!(
            parameters.get("hook"),
            Some(&meerschaum_core::Value::Str(expected.into())),
            "{keys}: {parameters:?}"
        );
    }
    session.shutdown();
}
