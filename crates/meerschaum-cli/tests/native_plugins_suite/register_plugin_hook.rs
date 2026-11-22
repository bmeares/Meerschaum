//! `register pipes` (and everything else that is `Pipe.register()`) asks a `plugin:` connector's
//! `register(pipe)` for the pipe's parameters and patches the pipe's own parameters onto them
//! (v4: `self.parameters = apply_patch_to_config(params, self.parameters)`). The expected
//! parameters below were recorded from v4.1.4 with the same synthetic plugins.
//!
//! Each test runs the built `mrsm-cli` binary against a temporary root, a SQLite instance and a
//! `PYTHONPATH` that points the Python worker at this repository.
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const REGISTER: &str = "def register(pipe, **kw):\n    return {\n        \
    'columns': {'datetime': 'dt', 'id': 'id'},\n        'dtypes': {'val': 'float'},\n        \
    'tags': ['from_plugin'],\n        'fetch': {'x': 1},\n    }\n\n\n\
    def fetch(pipe, **kw):\n    return []\n";

struct Setup {
    _dir: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
}

impl Setup {
    fn new(plugin: &str) -> Setup {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let plugins = dir.path().join("plugins");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&plugins).unwrap();
        std::fs::write(plugins.join("w17breg.py"), plugin).unwrap();
        Setup {
            _dir: dir,
            root,
            plugins,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo = repo.canonicalize().unwrap_or(repo);
        Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(args)
            .args(["-i", "sql:regtest"])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.root)
            .env("PYTHONPATH", repo)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env(
                "MRSM_SQL_REGTEST",
                format!("sqlite:///{}", self.root.join("regtest.db").display()),
            )
            .env("MRSM_NONINTERACTIVE", "1")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    /// Run `mrsm start mcp` (stdio), feed it `lines` and return what it printed.
    fn mcp(&self, lines: &[serde_json::Value]) -> String {
        use std::io::Write;
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo = repo.canonicalize().unwrap_or(repo);
        let mut child = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(["--noask", "start", "mcp", "-i", "sql:regtest"])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.root)
            .env("PYTHONPATH", repo)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env(
                "MRSM_SQL_REGTEST",
                format!("sqlite:///{}", self.root.join("regtest.db").display()),
            )
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        for line in lines {
            writeln!(stdin, "{line}").unwrap();
        }
        drop(stdin);
        let out = child.wait_with_output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// The registered parameters of `plugin:w17breg` / `demo`.
    fn parameters(&self) -> serde_json::Value {
        let out = self.run(&[
            "show",
            "pipes",
            "-c",
            "plugin:w17breg",
            "-m",
            "demo",
            "--nopretty",
        ]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        let line = text
            .lines()
            .find(|l| l.starts_with('{'))
            .unwrap_or_else(|| panic!("no pipe shown:\n{text}"));
        serde_json::from_str::<serde_json::Value>(line).unwrap()["parameters"].clone()
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap()
}

#[test]
fn the_plugins_register_parameters_become_the_pipes_parameters() {
    let setup = Setup::new(REGISTER);
    let out = setup.run(&["register", "pipes", "-c", "plugin:w17breg", "-m", "demo"]);
    assert!(
        text(&out).contains("Successfully registered 1 pipe"),
        "{}",
        text(&out)
    );
    assert_eq!(
        setup.parameters(),
        json(
            r#"{"columns": {"datetime": "dt", "id": "id"}, "dtypes": {"val": "float"},
                "fetch": {"x": 1}, "tags": ["from_plugin"]}"#
        )
    );
}

#[test]
fn the_command_line_wins_over_the_plugin() {
    let setup = Setup::new(REGISTER);
    let out = setup.run(&[
        "register",
        "pipes",
        "-c",
        "plugin:w17breg",
        "-m",
        "demo",
        "--tags",
        "cli1",
        "--params",
        r#"{"columns":{"datetime":"cli_dt"},"fetch":{"y":2}}"#,
    ]);
    assert!(
        text(&out).contains("Successfully registered 1 pipe"),
        "{}",
        text(&out)
    );
    assert_eq!(
        setup.parameters(),
        json(
            r#"{"columns": {"datetime": "cli_dt", "id": "id"}, "dtypes": {"val": "float"},
                "fetch": {"x": 1, "y": 2}, "tags": ["cli1"]}"#
        )
    );
}

#[test]
fn a_register_that_raises_returns_nothing_and_the_pipe_is_still_registered() {
    let setup = Setup::new(
        "def register(pipe, **kw):\n    raise ValueError('boom')\n\n\n\
         def fetch(pipe, **kw):\n    return []\n",
    );
    let out = setup.run(&["register", "pipes", "-c", "plugin:w17breg", "-m", "demo"]);
    assert!(
        text(&out).contains("Successfully registered 1 pipe"),
        "{}",
        text(&out)
    );
    assert!(text(&out).contains("boom"), "the traceback is shown");
    assert_eq!(
        setup.parameters(),
        json(r#"{"columns": {"datetime": null, "id": null}}"#)
    );
}

#[test]
fn a_register_that_returns_a_non_dict_warns_and_changes_nothing() {
    let setup = Setup::new(
        "def register(pipe, **kw):\n    return ['not', 'a', 'dict']\n\n\n\
         def fetch(pipe, **kw):\n    return []\n",
    );
    let out = setup.run(&["register", "pipes", "-c", "plugin:w17breg", "-m", "demo"]);
    let out_text = text(&out);
    assert!(
        out_text
            .contains("Invalid parameters returned from `register()` in connector plugin:w17breg"),
        "{out_text}"
    );
    assert!(
        out_text.contains("Successfully registered 1 pipe"),
        "{out_text}"
    );
    assert_eq!(
        setup.parameters(),
        json(r#"{"columns": {"datetime": null, "id": null}}"#)
    );
}

#[test]
fn a_plugin_without_register_changes_nothing() {
    let setup = Setup::new("def fetch(pipe, **kw):\n    return []\n");
    let out = setup.run(&["register", "pipes", "-c", "plugin:w17breg", "-m", "demo"]);
    assert!(
        text(&out).contains("Successfully registered 1 pipe"),
        "{}",
        text(&out)
    );
    assert_eq!(
        setup.parameters(),
        json(r#"{"columns": {"datetime": null, "id": null}}"#)
    );
}

#[test]
fn a_sql_pipe_never_starts_python() {
    let setup = Setup::new(REGISTER);
    // No plugin: nothing needs the worker, which cannot start (`no-python`).
    std::fs::remove_file(setup.plugins.join("w17breg.py")).unwrap();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let out = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args([
            "register",
            "pipes",
            "-c",
            "sql:regtest",
            "-m",
            "demo",
            "-i",
            "sql:regtest",
        ])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", &setup.root)
        .env("PYTHONPATH", repo)
        .env("MRSM_PYTHON", setup.root.join("no-python"))
        .env("MRSM_ROOT_DIR", &setup.root)
        .env("MRSM_PLUGINS_DIR", &setup.plugins)
        .env(
            "MRSM_SQL_REGTEST",
            format!("sqlite:///{}", setup.root.join("regtest.db").display()),
        )
        .env("MRSM_NONINTERACTIVE", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        text(&out).contains("Successfully registered 1 pipe"),
        "{}",
        text(&out)
    );
}

#[test]
fn the_mcp_register_pipe_tool_asks_the_plugin_too() {
    let setup = Setup::new(REGISTER);
    let out = setup.mcp(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "register_pipe", "arguments": {
            "connector_keys": "plugin:w17breg", "metric_key": "demo",
            "columns": {"datetime": "mcp_dt"}, "tags": ["mcp"],
        }},
    })]);
    assert!(out.contains("\"id\": 1"), "{out}");
    // The plugin's parameters are the base; what the tool was given wins (as with the CLI).
    assert_eq!(
        setup.parameters(),
        json(
            r#"{"columns": {"datetime": "mcp_dt", "id": "id"}, "dtypes": {"val": "float"},
                "fetch": {"x": 1}, "tags": ["mcp"]}"#
        )
    );
}

/// Kills the server it holds, whatever the test did.
struct Server(std::process::Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One HTTP/1.1 request with a JSON body; the whole response text.
fn post(port: u16, path: &str, body: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

#[test]
fn the_api_mcp_endpoint_asks_the_plugin_too() {
    let setup = Setup::new(REGISTER);
    let socket = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap_or(repo);
    let server = Server(
        Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(["--noask", "start", "api", "-i", "sql:regtest"])
            .args(["--host", "127.0.0.1", "-p", &port.to_string()])
            .args(["--no-auth", "--no-dash", "-w", "2"])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &setup.root)
            .env("PYTHONPATH", repo)
            .env("MRSM_ROOT_DIR", &setup.root)
            .env("MRSM_PLUGINS_DIR", &setup.plugins)
            .env(
                "MRSM_SQL_REGTEST",
                format!("sqlite:///{}", setup.root.join("regtest.db").display()),
            )
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut server = server;
    loop {
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "the server exited before listening"
        );
        let up = std::net::TcpStream::connect(("127.0.0.1", port)).is_ok();
        if up {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the server did not listen in time"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 5, "method": "tools/call",
        "params": {"name": "register_pipe", "arguments": {
            "connector_keys": "plugin:w17breg", "metric_key": "demo",
            "columns": {"datetime": "http_dt"}, "tags": ["http"],
        }},
    })
    .to_string();
    // The listener may accept before the app serves: retry the first request.
    let mut response = String::new();
    for _ in 0..40 {
        match post(port, "/mcp", &body) {
            Ok(text) if text.starts_with("HTTP/1.1 ") => {
                response = text;
                break;
            }
            _ => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    }
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"id\":5"), "{response}");
    drop(server);
    assert_eq!(
        setup.parameters(),
        json(
            r#"{"columns": {"datetime": "http_dt", "id": "id"}, "dtypes": {"val": "float"},
                "fetch": {"x": 1}, "tags": ["http"]}"#
        )
    );
}
