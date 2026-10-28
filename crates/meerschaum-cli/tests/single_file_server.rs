//! A single-file Rust plugin's server and job surfaces through the `mrsm` binary:
//!
//! - `#[api_route]` routes served by `mrsm start api` (path parameters, a suffix, scopes and the
//!   caller's principal) and `#[mcp_tool]` / `#[mcp_resource]` / `#[mcp_prompt]` entries listed
//!   and called over `/mcp`;
//! - a `#[make_executor]` module running jobs as local `mrsm` processes (`-e sfprocs:<label>`);
//! - a `#[make_connector]` module whose `fn subscribe` feeds `sync pipes --listen` and whose
//!   `fn ack` receives the committed tokens.
//!
//! Unix only (the executor detaches jobs with `sh`). Every run uses a temporary root and plugins
//! directory and a scrubbed environment; all data is synthetic.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use meerschaum_connector::DataQuery;
use meerschaum_connector_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};
use meerschaum_core::Pipe;
use serde_json::json;

#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;

const SOURCE: &str = r##"//! Routes, MCP entries, an executor and a stream in one file.
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::process::Command;

/// One synthetic item and who asked for it.
#[api_route(GET, "/sfx/items/{id:int}", scopes = ["pipes:read"])]
fn item(request: &RouteRequest) -> Value {
    json!({"id": request.path_param("id"), "q": request.query_param("q"),
        "principal": request.principal, "scopes": request.scopes})
}

#[api_route("/sfx/docs/{name}.json", public = true)]
fn doc(request: &RouteRequest) -> RouteResponse {
    RouteResponse::json(&json!({"name": request.path_param("name")})).header("x-sfx", "1")
}

#[api_route(POST, "/sfx/echo", scopes = ["pipes:write"])]
fn echo(request: &RouteRequest) -> (u16, Vec<u8>) {
    (201, request.body.clone())
}

/// Add two integers.
#[mcp_tool(input_schema = r#"{"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"], "additionalProperties": false}"#, read_only = true)]
fn sfx_add(call: &ToolCall) -> Result<Value> {
    let a = call.argument("a").and_then(Value::as_i64).unwrap_or(0);
    let b = call.argument("b").and_then(Value::as_i64).unwrap_or(0);
    Ok(json!({"sum": a + b, "user": call.principal.as_ref().and_then(|p| p.username.clone())}))
}

/// A secret only `sfx:secret` may call.
#[mcp_tool(scopes = ["sfx:secret"])]
fn sfx_secret() -> String {
    "synthetic".into()
}

/// A synthetic item.
#[mcp_resource(uri_template = "sfx://items/{id}")]
fn sfx_item(resource: &ResourceRead) -> ResourceContents {
    ResourceContents::json(&json!({"id": resource.variable("id")}))
}

#[mcp_prompt(description = "Greet someone.", arguments = ["who"])]
fn sfx_greet(arguments: &Value) -> Value {
    let who = arguments["who"].as_str().unwrap_or("nobody");
    json!({"description": format!("A greeting for {who}."), "messages": [format!("Say hello to {who}.")]})
}

/// Runs each job as a local `mrsm` process under `<root>/sfprocs_jobs/<label>/<name>`.
#[make_executor(name = "sfprocs")]
mod procs {
    use super::*;

    fn dir(executor: &ExecutorInfo, name: &str) -> PathBuf {
        PathBuf::from(executor.root_dir.clone().unwrap_or_default())
            .join("sfprocs_jobs")
            .join(&executor.label)
            .join(name)
    }

    fn pid(executor: &ExecutorInfo, name: &str) -> Option<u32> {
        let pid: u32 = std::fs::read_to_string(dir(executor, name).join("pid")).ok()?.trim().parse().ok()?;
        let alive = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|s| s.rsplit_once(") ").is_some_and(|(_, rest)| !rest.starts_with('Z')))
            .unwrap_or_else(|_| Command::new("kill").args(["-0", &pid.to_string()]).status().is_ok_and(|s| s.success()));
        alive.then_some(pid)
    }

    fn get_job_names(executor: &ExecutorInfo) -> Vec<String> {
        let base = dir(executor, "");
        let mut names: Vec<String> = std::fs::read_dir(base)
            .map(|entries| entries.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    fn get_job_metadata(executor: &ExecutorInfo, name: &str) -> Result<Value> {
        let Ok(text) = std::fs::read_to_string(dir(executor, name).join("sysargs.json")) else {
            return Ok(Value::Null);
        };
        let sysargs: Value = serde_json::from_str(&text).map_err(|e| Error::Message(e.to_string()))?;
        let pid = pid(executor, name);
        Ok(json!({"sysargs": sysargs, "result": [true, "Success"], "restart": false,
            "daemon": {"status": if pid.is_some() { "running" } else { "stopped" }, "pid": pid,
                "properties": {}}}))
    }

    fn create_job(executor: &ExecutorInfo, name: &str, sysargs: Vec<String>) -> Result<SuccessTuple> {
        std::fs::create_dir_all(dir(executor, name)).map_err(|e| Error::Message(e.to_string()))?;
        std::fs::write(dir(executor, name).join("sysargs.json"), json!(sysargs).to_string())
            .map_err(|e| Error::Message(e.to_string()))?;
        start_job(executor, name)
    }

    fn start_job(executor: &ExecutorInfo, name: &str) -> Result<SuccessTuple> {
        if pid(executor, name).is_some() {
            return Ok(SuccessTuple::ok(format!("Job '{name}' is already running.")));
        }
        let text = std::fs::read_to_string(dir(executor, name).join("sysargs.json"))
            .map_err(|e| Error::Message(e.to_string()))?;
        let sysargs: Vec<String> = serde_json::from_str(&text).map_err(|e| Error::Message(e.to_string()))?;
        let output = Command::new("sh")
            .arg("-c")
            .arg(r#""$0" "$@" >>"$SFPROCS_LOG" 2>&1 </dev/null & echo $!"#)
            .args(&executor.mrsm)
            .args(&sysargs)
            .env("SFPROCS_LOG", dir(executor, name).join("output.log"))
            .output()
            .map_err(|e| Error::Message(e.to_string()))?;
        let pid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        std::fs::write(dir(executor, name).join("pid"), &pid).map_err(|e| Error::Message(e.to_string()))?;
        Ok(SuccessTuple::ok(format!("Started job '{name}' (PID {pid}).")))
    }

    fn stop_job(executor: &ExecutorInfo, name: &str) -> (bool, String) {
        if let Some(pid) = pid(executor, name) {
            let _ = Command::new("kill").args(["-TERM", &pid.to_string()]).status();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while self::pid(executor, name).is_some() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        (true, format!("Stopped job '{name}'."))
    }

    fn delete_job(executor: &ExecutorInfo, name: &str) -> (bool, String) {
        stop_job(executor, name);
        let _ = std::fs::remove_dir_all(dir(executor, name));
        (true, format!("Deleted job '{name}'."))
    }

    fn get_logs(executor: &ExecutorInfo, name: &str) -> Option<String> {
        std::fs::read_to_string(dir(executor, name).join("output.log")).ok()
    }

    fn monitor_logs(executor: &ExecutorInfo, name: &str, stop_on_exit: bool) -> impl FnMut(Duration) -> LogEvent {
        let (executor, name) = (executor.clone(), name.to_owned());
        let mut offset = 0;
        move |timeout| {
            let mut text = String::new();
            if let Ok(mut file) = std::fs::File::open(dir(&executor, &name).join("output.log")) {
                let _ = file.seek(SeekFrom::Start(offset));
                let _ = file.read_to_string(&mut text);
            }
            offset += text.len() as u64;
            if !text.is_empty() {
                return LogEvent::Lines(text.lines().map(str::to_owned).collect());
            }
            if stop_on_exit && pid(&executor, &name).is_none() {
                return LogEvent::End(None);
            }
            std::thread::sleep(timeout.min(Duration::from_millis(100)));
            LogEvent::Timeout
        }
    }
}

/// Emits `fetch.count` one-row batches, each with its `id` as the acknowledgement token.
#[make_connector]
mod ticker {
    use super::*;

    fn subscribe(pipe: &Pipe, kwargs: &Value) -> Result<impl FnMut(Duration) -> Batch> {
        let count = pipe.parameters()["fetch"]["count"].as_u64().unwrap_or(5);
        let chunksize = kwargs["chunksize"].clone();
        let mut next = 0;
        Ok(move |timeout: Duration| {
            if next >= count {
                std::thread::sleep(timeout);
                return Batch::Timeout;
            }
            next += 1;
            std::thread::sleep(Duration::from_millis(100));
            Batch::RowsAck(vec![json!({"id": next, "chunksize": chunksize})], next)
        })
    }

    fn ack(tokens: &[u64]) -> Result<()> {
        let Some(path) = std::env::var_os("SFX_ACK_LOG") else { return Ok(()) };
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)
            .map_err(|e| Error::Message(e.to_string()))?;
        for token in tokens {
            writeln!(file, "{token}").map_err(|e| Error::Message(e.to_string()))?;
        }
        Ok(())
    }
}
"##;

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        };
        std::fs::create_dir_all(home.root.path().join("sqlite")).unwrap();
        std::fs::write(home.plugins.path().join("sfx.rs"), SOURCE).unwrap();
        let builds = home.root.path().join("cache/native_build");
        let found = meerschaum_plugin::discovery::discover_report_in(
            &[home.plugins.path().into()],
            &builds,
        );
        assert!(found.skipped.is_empty(), "{:?}", found.skipped);
        let candidate = &found.candidates[0];
        compile_single_file::compile(candidate);
        std::fs::write(
            builds.join("sfx/mrsm-source-fingerprint"),
            &candidate.fingerprint,
        )
        .unwrap();
        home
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NOASK", "true")
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"formatting":{"ansi":false,"unicode":false}}"#,
            );
        command
    }

    fn run(&self, args: &[&str]) -> String {
        let Output { stdout, stderr, .. } = self.command().args(args).output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            !text.contains("Building native plugin"),
            "the fixture build must be reused: {text}"
        );
        text
    }

    fn register_user(&self, username: &str) {
        let mut registering = self
            .command()
            .env_remove("MRSM_NOASK")
            .args([
                "register",
                "user",
                username,
                "-i",
                "sql:local",
                "--nopretty",
                "--scopes",
                "*",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        registering
            .stdin
            .take()
            .unwrap()
            .write_all(b"n\nsynthetic-pass-1\nsynthetic-pass-1\n\n")
            .unwrap();
        let output = registering.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }
}

fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
    let until = Instant::now() + deadline;
    while Instant::now() < until {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    done()
}

fn interrupt(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(15);
    while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
}

struct Server {
    child: Child,
    port: u16,
    stderr: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        interrupt(&mut self.child);
    }
}

/// `mrsm start api` on a random loopback port (never 8989), without the console.
fn serve(home: &Home) -> Server {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
    let stderr = home.root.path().join("server.err");
    let mut command = home.command();
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command
        .args([
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
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let mut server = Server {
        child,
        port,
        stderr,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited before listening: {}",
            std::fs::read_to_string(&server.stderr).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "server did not listen in time");
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

struct Reply {
    status: u16,
    head: String,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "not JSON ({e}): {} {}",
                self.head,
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

fn http(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Reply {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (key, value) in headers {
        head.push_str(&format!("{key}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a response head");
    let head = String::from_utf8_lossy(&response[..split]).into_owned();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    let mut body = response[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body);
    }
    Reply { status, head, body }
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = data.windows(2).position(|w| w == b"\r\n").unwrap();
        let size =
            usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap().trim(), 16)
                .unwrap();
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&data[line_end + 2..line_end + 2 + size]);
        data = &data[line_end + 2 + size + 2..];
    }
}

fn login(port: u16, scope: Option<&str>) -> String {
    let mut form = "username=ada&password=synthetic-pass-1".to_string();
    if let Some(scope) = scope {
        form.push_str(&format!("&scope={scope}"));
    }
    let reply = http(
        port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    );
    format!(
        "Bearer {}",
        reply.json()["access_token"].as_str().expect("a token")
    )
}

fn mcp(port: u16, auth: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let reply = http(
        port,
        "POST",
        "/mcp",
        &[
            ("Authorization", auth),
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
        ],
        body.to_string().as_bytes(),
    );
    assert_eq!(
        reply.status,
        200,
        "{}",
        String::from_utf8_lossy(&reply.body)
    );
    reply.json()
}

#[test]
fn single_file_routes_and_mcp_entries_are_served_by_start_api() {
    let home = Home::new();
    home.register_user("ada");
    let server = serve(&home);
    let port = server.port;

    // Scopes are enforced before the plugin runs; the plugin sees the principal and scopes.
    assert_eq!(http(port, "GET", "/sfx/items/7", &[], b"").status, 401);
    let reader = login(port, Some("pipes:read"));
    let item = http(
        port,
        "GET",
        "/sfx/items/7?q=a",
        &[("Authorization", &reader)],
        b"",
    );
    assert_eq!(item.status, 200, "{}", String::from_utf8_lossy(&item.body));
    let body = item.json();
    assert_eq!(body["id"], "7");
    assert_eq!(body["q"], "a");
    assert_eq!(body["principal"]["username"], "ada");
    assert_eq!(body["scopes"], json!(["pipes:read"]));
    // The `int` converter refuses a non-integer.
    assert_eq!(
        http(
            port,
            "GET",
            "/sfx/items/x",
            &[("Authorization", &reader)],
            b""
        )
        .status,
        404
    );
    assert_eq!(
        http(
            port,
            "POST",
            "/sfx/echo",
            &[("Authorization", &reader)],
            b"hi"
        )
        .status,
        403
    );
    let writer = login(port, None);
    let echo = http(
        port,
        "POST",
        "/sfx/echo",
        &[("Authorization", &writer)],
        &[0, 159, 255],
    );
    assert_eq!((echo.status, echo.body), (201, vec![0, 159, 255]));
    // A public route with a suffix and a response header.
    let doc = http(port, "GET", "/sfx/docs/a.b.json", &[], b"");
    assert_eq!(doc.json(), json!({"name": "a.b"}));
    assert!(
        doc.head.to_ascii_lowercase().contains("x-sfx: 1"),
        "{}",
        doc.head
    );

    // MCP: listed per scope, called with validated arguments.
    let tools = mcp(port, &reader, "tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"sfx_add"), "{names:?}");
    assert!(!names.contains(&"sfx_secret"), "{names:?}");
    let sum = mcp(
        port,
        &reader,
        "tools/call",
        json!({"name": "sfx_add", "arguments": {"a": 2, "b": 40}}),
    );
    assert_eq!(sum["result"]["isError"], false, "{sum}");
    let text = sum["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(text).unwrap(),
        json!({"sum": 42, "user": "ada"})
    );
    let invalid = mcp(
        port,
        &reader,
        "tools/call",
        json!({"name": "sfx_add", "arguments": {"a": 2}}),
    );
    assert_eq!(invalid["result"]["isError"], true, "{invalid}");
    let item = mcp(
        port,
        &reader,
        "resources/read",
        json!({"uri": "sfx://items/9"}),
    );
    assert!(
        item["result"]["contents"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"9\""),
        "{item}"
    );
    let greet = mcp(
        port,
        &reader,
        "prompts/get",
        json!({"name": "sfx_greet", "arguments": {"who": "Ada"}}),
    );
    assert_eq!(greet["result"]["description"], "A greeting for Ada.");
    assert_eq!(
        greet["result"]["messages"][0]["content"]["text"],
        "Say hello to Ada."
    );
}

fn pid_alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| !rest.starts_with('Z'))
        })
        .unwrap_or(false)
}

/// Kills a job process left behind by a failed assertion.
struct Reaper(Option<u32>);
impl Drop for Reaper {
    fn drop(&mut self) {
        if let Some(pid) = self.0
            && pid_alive(pid)
        {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    }
}

#[test]
fn single_file_executor_runs_lists_logs_stops_and_deletes_jobs() {
    let home = Home::new();
    let created = home.run(&[
        "show",
        "version",
        "-s",
        "every 1 seconds",
        "-d",
        "-e",
        "sfprocs:test",
        "--name",
        "ticktock",
        "-y",
    ]);
    assert!(
        created.contains("Successfully started job 'ticktock'."),
        "{created}"
    );
    // The executor received the root the jobs belong to and the sysargs without `-d` / `-e`.
    let dir = home.root.path().join("sfprocs_jobs/test/ticktock");
    let pid: u32 = std::fs::read_to_string(dir.join("pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let _reaper = Reaper(Some(pid));
    let sysargs: Vec<String> =
        serde_json::from_str(&std::fs::read_to_string(dir.join("sysargs.json")).unwrap()).unwrap();
    assert_eq!(&sysargs[..4], ["show", "version", "-s", "every 1 seconds"]);
    assert!(
        !sysargs.iter().any(|a| a == "-e" || a == "-d"),
        "{sysargs:?}"
    );

    let listed = home.run(&["show", "jobs", "-e", "sfprocs:test"]);
    assert!(listed.contains("ticktock"), "{listed}");
    assert!(listed.contains("running"), "{listed}");
    assert!(
        wait_for(Duration::from_secs(60), || {
            home.run(&[
                "show",
                "logs",
                "ticktock",
                "-e",
                "sfprocs:test",
                "--nopretty",
            ])
            .matches("Meerschaum v")
            .count()
                >= 2
        }),
        "the job never logged:\n{}",
        home.run(&[
            "show",
            "logs",
            "ticktock",
            "-e",
            "sfprocs:test",
            "--nopretty"
        ])
    );
    // A method the module leaves out is reported as not supported.
    let paused = home.run(&["pause", "jobs", "ticktock", "-e", "sfprocs:test", "-y"]);
    assert!(
        paused.contains("`pause_job` is not supported by the 'sfprocs:test' executor."),
        "{paused}"
    );
    let stopped = home.run(&["stop", "jobs", "ticktock", "-e", "sfprocs:test", "-y"]);
    assert!(stopped.contains("Stopped 1 job."), "{stopped}");
    assert!(wait_for(Duration::from_secs(10), || !pid_alive(pid)));
    let deleted = home.run(&["delete", "jobs", "ticktock", "-e", "sfprocs:test", "-y"]);
    assert!(deleted.contains("Deleted job 'ticktock'."), "{deleted}");
    assert!(!dir.exists());
    // The local executor never saw the job.
    assert!(!home.root.path().join("jobs").join("ticktock").exists());
}

fn sqlite(rt: &tokio::runtime::Runtime, root: &Path) -> SqlConnector {
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            root.join("sqlite/mrsm_local.db")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    rt.block_on(SqlConnector::connect(
        "local",
        ConnectionSpec::from_attributes(&attrs).unwrap(),
    ))
    .unwrap()
}

#[test]
fn single_file_subscribe_feeds_a_listen_sync_and_receives_acks() {
    let home = Home::new();
    let acks = home.root.path().join("acks.log");
    let registered = home.run(&[
        "register",
        "pipes",
        "-c",
        "ticker:test",
        "-m",
        "readings",
        "-i",
        "sql:local",
        "--params",
        r#"{"fetch": {"count": 5}, "columns": {"datetime": "id"}, "dtypes": {"id": "int"}}"#,
    ]);
    assert!(
        registered.contains("Successfully registered"),
        "{registered}"
    );
    let log_path = home.root.path().join("listen.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = home
        .command()
        .env("SFX_ACK_LOG", &acks)
        .args([
            "sync",
            "pipes",
            "-c",
            "ticker:test",
            "-i",
            "sql:local",
            "--listen",
            "--chunksize",
            "2",
        ])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let conn = sqlite(&rt, home.root.path());
    let pipe = Pipe::builder("ticker:test", "readings")
        .instance("sql:local")
        .build()
        .unwrap();
    let rowcount = || {
        rt.block_on(conn.get_pipe_rowcount(&pipe, &DataQuery::default()))
            .ok()
            .flatten()
            .unwrap_or(0)
    };
    let landed = wait_for(Duration::from_secs(90), || rowcount() >= 5);
    wait_for(Duration::from_secs(10), || {
        std::fs::read_to_string(&acks)
            .unwrap_or_default()
            .lines()
            .count()
            >= 5
    });
    interrupt(&mut child);
    let output = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(landed, "rows: {}\n{output}", rowcount());
    assert_eq!(rowcount(), 5, "{output}");
    assert!(output.contains("Listening for new data"), "{output}");
    // `subscribe` received the sync's keyword arguments.
    let data = rt
        .block_on(conn.get_pipe_data(&pipe, &DataQuery::default()))
        .unwrap()
        .unwrap();
    let chunksize = data
        .columns()
        .iter()
        .position(|c| c.name == "chunksize")
        .expect("a chunksize column");
    assert_eq!(
        data.row(0)[chunksize],
        meerschaum_kernel::Value::Int(2),
        "{output}"
    );
    let mut acked: Vec<u64> = std::fs::read_to_string(&acks)
        .unwrap()
        .lines()
        .map(|l| l.parse().unwrap())
        .collect();
    acked.sort_unstable();
    acked.dedup();
    assert_eq!(acked, vec![1, 2, 3, 4, 5], "{output}");
}
