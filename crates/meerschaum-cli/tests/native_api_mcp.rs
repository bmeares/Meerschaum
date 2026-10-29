//! A packaged native plugin's API routes and MCP entries through the `mrsm` binary
//! (`examples/native_api.rs` of `meerschaum-plugin`): path parameters, headers, byte bodies,
//! scopes, the refusal of a route that would shadow a core route, and MCP list/call over HTTP
//! (`/mcp`) and stdio (`mrsm start mcp`). All data is synthetic; every run uses a temporary root
//! and plugins directory and a scrubbed environment.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
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
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

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
        let dir = home.plugins.path().join("native_api");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("plugin.toml"),
            "name = \"native_api\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        std::fs::copy(
            library("native_api"),
            dir.join(meerschaum_plugin::discovery::library_name("native_api")),
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
            .env("MRSM_NO_UPDATE_CHECK", "1");
        command
    }

    fn register_user(&self, username: &str) {
        let mut registering = self
            .command()
            .args([
                "register",
                "user",
                username,
                "-i",
                "sql:local",
                "--nopretty",
                "--role",
                "operator",
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
            // Keep the first fixture user non-admin; Operator has no custom plugin scope.
            .write_all(b"n\nsynthetic-pass-1\nsynthetic-pass-1\n\n")
            .unwrap();
        let output = registering.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }
}

struct Server {
    child: Child,
    port: u16,
    stderr: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
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
fn serve(home: &Home) -> Server {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
    let stderr = home.root.path().join("server.err");
    let mut command = home.command();
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command
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
            "server exited before listening"
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
    fn headers(&self, name: &str) -> Vec<String> {
        self.head
            .lines()
            .filter_map(|l| l.split_once(':'))
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_string())
            .collect()
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
    let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
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
fn native_routes_and_mcp_are_served_by_start_api() {
    use serde_json::json;
    let home = Home::new();
    home.register_user("ada");
    let server = serve(&home);
    let port = server.port;

    // The route that would shadow `/pipes` is refused at load, with the reason.
    let notices = std::fs::read_to_string(&server.stderr).unwrap();
    assert!(
        notices.contains(
            "Native plugin 'native_api': route GET /pipes/native-api is refused: it would shadow the core route group 'pipes'."
        ),
        "{notices}"
    );

    // An MCP entry named like a built-in is hidden, with a notice.
    assert!(
        notices.contains(
            "Native plugin 'native_api': MCP tool 'list_pipes' is hidden: a built-in tool has that name."
        ),
        "{notices}"
    );

    // Server calls run concurrently: a slow route holds up neither other routes nor MCP calls.
    let slow = std::thread::spawn(move || http(port, "GET", "/native-api/slow", &[], b""));
    std::thread::sleep(Duration::from_millis(300));
    let started = Instant::now();
    let quick = http(port, "GET", "/native-api/whoami", &[], b"");
    assert_eq!(quick.status, 200);
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "a slow route blocked another one: {:?}",
        started.elapsed()
    );
    assert_eq!(slow.join().unwrap().body, b"slept");

    // Public: no credentials; the plugin sees no principal.
    let whoami = http(port, "GET", "/native-api/whoami", &[], b"");
    assert_eq!(whoami.status, 200);
    assert_eq!(whoami.json(), json!({"principal": null, "scopes": []}));

    // Path parameters, the query, headers and the principal; the scope is enforced.
    let denied = http(port, "GET", "/native-api/items/7", &[], b"");
    assert_eq!(denied.status, 401);
    let reader = login(port, Some("pipes:read"));
    let item = http(
        port,
        "GET",
        "/native-api/items/7?q=a&q=b",
        &[("Authorization", &reader), ("X-Native-Api", "yes")],
        b"",
    );
    assert_eq!(item.status, 200, "{}", String::from_utf8_lossy(&item.body));
    let body = item.json();
    assert_eq!(body["id"], "7");
    assert_eq!(body["route_path"], "/native-api/items/{id}");
    assert_eq!(body["q"], json!(["a", "b"]));
    assert_eq!(body["header"], "yes");
    assert_eq!(body["has_authorization"], false);
    assert_eq!(body["principal"]["kind"], "user");
    assert_eq!(body["principal"]["username"], "ada");

    // Bytes in and out, with response headers; `pipes:write` is missing from the reader's JWT.
    let bytes = [0u8, 159, 146, 150, 255];
    let forbidden = http(
        port,
        "PUT",
        "/native-api/blobs/a/b",
        &[("Authorization", &reader)],
        &bytes,
    );
    assert_eq!(forbidden.status, 403);
    let writer = login(port, None);
    let blob = http(
        port,
        "PUT",
        "/native-api/blobs/a/b",
        &[("Authorization", &writer)],
        &bytes,
    );
    assert_eq!(blob.status, 201);
    assert_eq!(blob.body, bytes);
    assert_eq!(blob.headers("x-native-key"), vec!["a/b".to_string()]);
    assert_eq!(blob.headers("set-cookie").len(), 2);

    // `*`: any method.
    let any = http(
        port,
        "PATCH",
        "/native-api/any",
        &[("Authorization", &writer)],
        b"",
    );
    assert_eq!(
        (any.status, any.body.as_slice()),
        (202, b"PATCH".as_slice())
    );

    // A parameter with a suffix; cookies reach the plugin.
    let doc = http(port, "GET", "/native-api/docs/a.b.json", &[], b"");
    assert_eq!(doc.json(), json!({"name": "a.b"}));
    let prefs = http(
        port,
        "GET",
        "/native-api/prefs",
        &[("Cookie", "theme=dark; other=1")],
        b"",
    );
    assert_eq!(prefs.json(), json!({"theme": "dark"}));

    // An API key (a token principal) reaches routes and MCP entries; plugins see who called.
    let registered = http(
        port,
        "POST",
        "/tokens/register",
        &[
            ("Authorization", &writer),
            ("Content-Type", "application/json"),
        ],
        br#"{"label": "native-key", "scopes": ["pipes:read"]}"#,
    );
    assert_eq!(
        registered.status,
        200,
        "{}",
        String::from_utf8_lossy(&registered.body)
    );
    let key = format!(
        "Bearer {}",
        registered.json()["api_key"].as_str().expect("an API key")
    );
    let keyed = http(
        port,
        "GET",
        "/native-api/items/8",
        &[("Authorization", &key)],
        b"",
    );
    assert_eq!(
        keyed.status,
        200,
        "{}",
        String::from_utf8_lossy(&keyed.body)
    );
    assert_eq!(keyed.json()["principal"]["kind"], "token");
    assert_eq!(keyed.json()["principal"]["label"], "native-key");
    let whoami = |auth: &str| {
        let reply = mcp(
            port,
            auth,
            "tools/call",
            json!({"name": "native_api_whoami"}),
        );
        assert_eq!(reply["result"]["isError"], false, "{reply}");
        let text = reply["result"]["content"][0]["text"].as_str().unwrap();
        serde_json::from_str::<serde_json::Value>(text).unwrap()
    };
    let by_key = whoami(&key);
    assert_eq!(by_key["principal"]["kind"], "token");
    assert_eq!(by_key["principal"]["label"], "native-key");
    assert_eq!(by_key["scopes"], json!(["pipes:read"]));
    let by_user = whoami(&reader);
    assert_eq!(by_user["principal"]["kind"], "user");
    assert_eq!(by_user["principal"]["username"], "ada");

    // MCP: native tools are listed per scope and called with validated arguments.
    let tools = mcp(port, &reader, "tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"native_api_add"), "{names:?}");
    assert!(!names.contains(&"native_api_secret"), "{names:?}");
    let sum = mcp(
        port,
        &reader,
        "tools/call",
        json!({"name": "native_api_add", "arguments": {"a": 2, "b": 40}}),
    );
    assert_eq!(sum["result"]["isError"], false, "{sum}");
    let text = sum["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(text).unwrap(),
        json!({"sum": 42})
    );
    let invalid = mcp(
        port,
        &reader,
        "tools/call",
        json!({"name": "native_api_add", "arguments": {"a": 2, "b": "x"}}),
    );
    assert_eq!(invalid["result"]["isError"], true);
    assert!(
        invalid["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Invalid arguments for 'native_api_add'"),
        "{invalid}"
    );
    let secret = mcp(
        port,
        &writer,
        "tools/call",
        json!({"name": "native_api_secret"}),
    );
    // A custom scope is not among a user's default scopes: the call is refused like any tool's.
    assert_eq!(secret["result"]["isError"], true);
    assert!(
        secret["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("native:secret"),
        "{secret}"
    );

    let item = mcp(
        port,
        &reader,
        "resources/read",
        json!({"uri": "native-api://items/9"}),
    );
    assert!(
        item["result"]["contents"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"9\""),
        "{item}"
    );
    let logo = mcp(
        port,
        &reader,
        "resources/read",
        json!({"uri": "native-api://logo"}),
    );
    assert_eq!(logo["result"]["contents"][0]["blob"], "iU1SAA==");
    let greet = mcp(
        port,
        &reader,
        "prompts/get",
        json!({"name": "native_api_greet", "arguments": {"who": "Ada"}}),
    );
    assert_eq!(greet["result"]["description"], "A greeting for Ada.");
}

#[test]
fn native_mcp_tools_are_served_over_stdio() {
    let home = Home::new();
    let mut child = home
        .command()
        .args(["--noask", "start", "mcp", "-i", "sql:local"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let requests = [
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"native_api_add","arguments":{"a":1,"b":2}}}"#,
    ];
    for request in requests {
        writeln!(stdin, "{request}").unwrap();
    }
    stdin.flush().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut found = None;
    for line in lines.by_ref().take(2) {
        let value: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        if value["id"] == 2 {
            found = Some(value);
        }
    }
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    let found = found.expect("a tools/call response");
    let text = found["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(text).unwrap(),
        serde_json::json!({"sum": 3})
    );
}
