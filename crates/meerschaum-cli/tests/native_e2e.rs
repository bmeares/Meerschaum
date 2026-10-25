//! Native (C-ABI) plugins end to end through the `mrsm` binary, beyond `native_plugins.rs`:
//! sync hooks around a real sync, routes served by `mrsm start api`, a native plugin shadowing a
//! Python plugin, load-time rejection (corrupt library, wrong ABI) and the manifest cache.
//! Plugins are the SDK examples (`native_demo`, `native_probe`, `bad_abi`); all data is synthetic.
//! Every run uses a temporary root and plugins directory and a scrubbed environment.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

fn library(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "meerschaum-plugin", "--examples"])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    // The examples land in the shared target directory next to this test's executable.
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// An interpreter for tests that need the Python worker (`MRSM_TEST_HOOKS_PYTHON`, `.venv`,
/// then the reference environment); `None` skips them.
fn python() -> Option<PathBuf> {
    std::env::var_os("MRSM_TEST_HOOKS_PYTHON")
        .map(PathBuf::from)
        .into_iter()
        .chain([
            repository().join(".venv/bin/python"),
            repository().join(".venv-ref/bin/python"),
        ])
        .find(|path| path.is_file())
}

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
    scratch: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
            scratch: tempfile::tempdir().unwrap(),
        }
    }

    /// Install an SDK example as the plugin directory `<plugins>/<name>/`.
    fn install(&self, example: &str, name: &str) -> PathBuf {
        let dir = self.plugins.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("plugin.toml"),
            format!("name = \"{name}\"\nversion = \"1.0.0\"\n"),
        )
        .unwrap();
        std::fs::copy(
            library(example),
            dir.join(meerschaum_plugin::discovery::library_name(name)),
        )
        .unwrap();
        dir
    }

    fn log(&self) -> PathBuf {
        self.scratch.path().join("hooks.log")
    }

    fn logged(&self) -> Vec<String> {
        std::fs::read_to_string(self.log())
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn command(&self, python: Option<&Path>) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("NATIVE_PROBE_LOG", self.log())
            .env("PYTHONPATH", repository());
        if let Some(python) = python {
            command
                .env("MRSM_PYTHON", python)
                .env("UV_PYTHON", python)
                .env(
                    "MRSM_CONFIG",
                    r#"{"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false},"system":{"engine":{"python_workers":1}}}"#,
                );
        }
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(None).args(args).output().unwrap()
    }

    fn run_env(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        self.command(None)
            .args(args)
            .envs(env.iter().copied())
            .output()
            .unwrap()
    }

    fn register(&self, source: &str, metric: &str) {
        let output = self.run(&[
            "register",
            "pipe",
            "-c",
            source,
            "-m",
            metric,
            "-i",
            "sql:local",
            "--noask",
            "--nopretty",
            "--params",
            r#"{"columns":{"id":"id"}}"#,
        ]);
        assert!(output.status.success(), "{}", text(&output));
    }

    fn sync(&self, source: &str, metric: &str, env: &[(&str, &str)]) -> Output {
        self.run_env(
            &[
                "sync",
                "pipes",
                "-c",
                source,
                "-m",
                metric,
                "-i",
                "sql:local",
                "--nopretty",
            ],
            env,
        )
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn native_hooks_fire_around_each_real_sync_and_the_builtin_sync_writes_the_rows() {
    let home = Home::new();
    home.install("native_probe", "native_probe");
    home.register("plugin:native_probe", "probe");

    // The probe declares `fetch` but no `sync`: the built-in sync stores the fetched frame.
    let first = home.sync("plugin:native_probe", "probe", &[]);
    assert!(first.status.success(), "{}", text(&first));
    assert!(
        text(&first).contains("Inserted 3, updated 0 rows."),
        "{}",
        text(&first)
    );
    assert_eq!(
        home.logged(),
        ["pre metric=\"probe\"", "post metric=\"probe\" success=true"]
    );

    // The same three rows again: the diff finds nothing new, and the hooks still run.
    let second = home.sync("plugin:native_probe", "probe", &[]);
    assert!(
        text(&second).contains("Inserted 0, updated 0 rows."),
        "{}",
        text(&second)
    );
    assert_eq!(
        home.logged(),
        [
            "pre metric=\"probe\"",
            "post metric=\"probe\" success=true",
            "pre metric=\"probe\"",
            "post metric=\"probe\" success=true",
        ]
    );
}

#[test]
fn a_failing_native_hook_warns_and_never_fails_the_sync() {
    let home = Home::new();
    home.install("native_probe", "native_probe");
    home.register("plugin:native_probe", "probe");
    for (phase, hook) in [("pre", "before"), ("post", "after")] {
        let output = home.sync(
            "plugin:native_probe",
            "probe",
            &[("NATIVE_PROBE_FAIL", phase)],
        );
        let out = text(&output);
        assert!(output.status.success(), "{out}");
        assert!(
            out.contains(&format!(
                "Warning: Native plugin 'native_probe' hook failed: {hook}: PROBE_{}_FAILED",
                phase.to_uppercase()
            )),
            "{out}"
        );
    }
    // Both hooks ran on both syncs (the failure of one never skips the other).
    assert_eq!(home.logged().len(), 4, "{:?}", home.logged());
}

#[test]
fn a_native_instance_syncs_its_pipes_and_leases_fall_back_to_files() {
    let home = Home::new();
    home.install("native_demo", "native_demo");
    // `native_demo:memory` is the plugin's own instance: it lists its pipes and syncs them.
    let output = home.run(&["sync", "pipes", "-i", "native_demo:memory", "--nopretty"]);
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    assert!(
        out.contains(r#""connector_keys": "native_demo:source", "metric_key": "plugin_abi""#)
            && out.contains(r#""success": true"#),
        "{out}"
    );
    // The demo answers no lease methods, so the engine falls back to file locks and says so.
    assert!(out.contains("does not implement pipe leases"), "{out}");
}

#[test]
fn plan_and_graph_resolve_a_native_source() {
    let home = Home::new();
    home.install("native_demo", "native_demo");
    home.register("plugin:native_demo", "demo");
    let args = [
        "-c",
        "plugin:native_demo",
        "-m",
        "demo",
        "-i",
        "sql:local",
        "--nopretty",
    ];
    for action in ["plan", "show"] {
        let mut full = vec![action, if action == "plan" { "pipes" } else { "graph" }];
        full.extend(args);
        let output = home.run(&full);
        let out = text(&output);
        assert!(output.status.success(), "{out}");
        assert!(
            out.contains("sql:local|plugin:native_demo|demo|None"),
            "{out}"
        );
    }
}

#[test]
fn a_corrupt_or_wrong_abi_library_is_skipped_and_the_rest_keep_working() {
    let home = Home::new();
    home.install("native_demo", "native_demo");
    // A wrong-ABI table (version 99) and a file that is not a library at all.
    home.install("bad_abi", "bad_abi");
    let corrupt = home.plugins.path().join("corrupt");
    std::fs::create_dir_all(&corrupt).unwrap();
    std::fs::write(
        corrupt.join("plugin.toml"),
        "name = \"corrupt\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    std::fs::write(
        corrupt.join(meerschaum_plugin::discovery::library_name("corrupt")),
        "not a library",
    )
    .unwrap();

    let output = home.run(&["hello"]);
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    assert!(out.contains("Hello from native_demo"), "{out}");
    assert!(out.contains("Skipping invalid native plugin"), "{out}");
    assert!(out.contains("ABI 99"), "{out}");
    // Listing never loads a library: an invalid plugin is still installed (as a broken Python
    // plugin is), so `uninstall plugins` can name it.
    let plugins =
        String::from_utf8_lossy(&home.run(&["show", "plugins", "--nopretty"]).stdout).into_owned();
    assert_eq!(plugins, "bad_abi\ncorrupt\nnative_demo\n");
}

#[test]
fn the_native_manifest_is_cached_and_follows_the_plugin_directory() {
    let home = Home::new();
    let dir = home.install("native_demo", "native_demo");
    assert!(text(&home.run(&["hello"])).contains("Hello from native_demo"));
    let cache = home.root.path().join("cache/native_plugins_manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cache).expect("the manifest cache was written"))
            .unwrap();
    assert!(
        manifest["plugins"].get("native_demo").is_some(),
        "{manifest}"
    );
    // Replacing the plugin directory invalidates the cached entry on the next run.
    std::fs::remove_dir_all(dir).unwrap();
    let actions = text(&home.run(&["show", "actions", "--nopretty"]));
    assert!(!actions.lines().any(|l| l.trim() == "hello"), "{actions}");
}

#[test]
fn a_native_plugin_replaces_a_python_plugin_of_the_same_name() {
    let Some(python) = python() else {
        eprintln!("skipped: a Python environment is required for the Python plugin");
        return;
    };
    let source = "from meerschaum.plugins import make_action\n__version__ = '0.0.1'\n\
        @make_action\ndef hello(**kw):\n    return True, 'Hello from PYTHON native_demo'\n\
        @make_action\ndef pyonly(**kw):\n    return True, 'Python-only action'\n";
    let side = "from meerschaum.plugins import make_action\n__version__ = '0.0.1'\n\
        @make_action\ndef pyside(**kw):\n    return True, 'Hello from py_side'\n";
    let prepare = |home: &Home| {
        // Share the interpreter's packages read-only so no venv is built during the test.
        #[cfg(unix)]
        for name in ["mrsm", "native_demo", "py_side"] {
            std::fs::create_dir_all(home.root.path().join("venvs")).unwrap();
            std::os::unix::fs::symlink(
                python.parent().unwrap().parent().unwrap(),
                home.root.path().join("venvs").join(name),
            )
            .unwrap();
        }
        std::fs::write(home.plugins.path().join("py_side.py"), side).unwrap();
    };
    let run = |home: &Home, args: &[&str]| home.command(Some(&python)).args(args).output().unwrap();

    // Control: with no native library the Python `hello` is the one that runs.
    let control = Home::new();
    prepare(&control);
    std::fs::write(control.plugins.path().join("native_demo.py"), source).unwrap();
    assert!(text(&run(&control, &["hello"])).contains("Hello from PYTHON native_demo"));

    // A directory holding both `plugin.toml` and Python files is the native plugin.
    let home = Home::new();
    prepare(&home);
    let dir = home.install("native_demo", "native_demo");
    std::fs::write(dir.join("__init__.py"), source).unwrap();
    let hello = text(&run(&home, &["hello"]));
    assert!(hello.contains("Hello from native_demo"), "{hello}");
    assert!(!hello.contains("PYTHON"), "{hello}");
    // The whole Python plugin is replaced, not merged: its other actions are gone...
    let pyonly = run(&home, &["pyonly"]);
    assert!(!pyonly.status.success() && !text(&pyonly).contains("Python-only action"));
    // ...while an unrelated Python plugin keeps working beside the native one.
    assert!(text(&run(&home, &["pyside"])).contains("Hello from py_side"));
    let listing = text(&run(&home, &["show", "plugins"]));
    assert!(
        listing.contains("native_demo (native)") && listing.contains("py_side"),
        "{listing}"
    );
}

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        #[cfg(unix)]
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-INT", &self.child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(3);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `mrsm start api` on a random loopback port (never 8989), without the console.
fn serve(home: &Home, extra: &[&str]) -> Server {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
    let mut command = home.command(None);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command
        .args([
            "--noask",
            "start",
            "api",
            "-i",
            "sql:local",
            "--host",
            "127.0.0.1",
            "-p",
        ])
        .arg(port.to_string())
        .args(["--no-dash", "-w", "2"])
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            child.try_wait().unwrap().is_none(),
            "server exited before listening"
        );
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server did not listen before timeout"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    Server { child, port }
}

fn http(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (key, value) in headers {
        head.push_str(&format!("{key}: {value}\r\n"));
    }
    write!(stream, "{head}Content-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn json_body(response: &str) -> serde_json::Value {
    serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap_or_default().trim())
        .unwrap_or_else(|e| panic!("not JSON ({e}): {response}"))
}

#[test]
fn native_routes_are_served_with_the_core_apis_credentials() {
    let home = Home::new();
    home.install("native_demo", "native_demo");
    // The password prompt (and the optional email) read stdin.
    let mut registering = home
        .command(None)
        .args(["register", "user", "ada", "-i", "sql:local", "--nopretty"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    registering
        .stdin
        .take()
        .unwrap()
        .write_all(b"synthetic-pass-1\nsynthetic-pass-1\n\n")
        .unwrap();
    let registered = registering.wait_with_output().unwrap();
    assert!(
        text(&registered).contains("Successfully registered user 'ada'"),
        "{}",
        text(&registered)
    );

    let server = serve(&home, &[]);
    // `"public": true` needs no credentials; the plugin sees the request as the host built it.
    let ping = http(server.port, "GET", "/native-demo/ping", &[], "");
    assert!(ping.starts_with("HTTP/1.1 200"), "{ping}");
    assert!(ping.contains("content-type: application/json"), "{ping}");
    let body = json_body(&ping);
    assert_eq!(
        (body["method"].as_str(), body["path"].as_str()),
        (Some("GET"), Some("/native-demo/ping"))
    );
    assert!(body["user"].is_null());

    // The other route requires the core API's credentials.
    let denied = http(server.port, "POST", "/native-demo/echo", &[], "hi");
    assert!(denied.starts_with("HTTP/1.1 401"), "{denied}");
    let login = http(
        server.port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        "username=ada&password=synthetic-pass-1",
    );
    let token = json_body(&login)["access_token"]
        .as_str()
        .expect("a token")
        .to_string();
    let bearer = format!("Bearer {token}");
    let echoed = http(
        server.port,
        "POST",
        "/native-demo/echo",
        &[("Authorization", &bearer)],
        "hi",
    );
    assert!(echoed.starts_with("HTTP/1.1 200"), "{echoed}");
    let body = json_body(&echoed);
    assert_eq!(
        (body["body"].as_str(), body["user"].as_str()),
        (Some("hi"), Some("ada"))
    );

    // Routes a plugin does not list are not served.
    let missing = http(server.port, "GET", "/native-demo/other", &[], "");
    assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");
}

#[test]
fn native_routes_need_no_credentials_when_the_server_runs_with_no_auth() {
    let home = Home::new();
    home.install("native_demo", "native_demo");
    let server = serve(&home, &["--no-auth"]);
    let echoed = http(server.port, "POST", "/native-demo/echo", &[], "{\"a\":1}");
    assert!(echoed.starts_with("HTTP/1.1 200"), "{echoed}");
    let body = json_body(&echoed);
    assert_eq!(body["body"], "{\"a\":1}");
    assert!(body["user"].is_null());
}
