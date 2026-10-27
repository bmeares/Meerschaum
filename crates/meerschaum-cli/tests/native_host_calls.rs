//! Native plugins talking to Meerschaum through the `mrsm` binary (host calls): a single-file
//! plugin whose `sync` reads a parent pipe's data, counts its rows with `read_sql` on the
//! instance, rewrites its own window (clear and sync, or one `replace`), and reads the plugin
//! config its action wrote; a `register` hook that reads it too; actions with `host::pipe(...)`
//! and `exec_sql`; and routes whose host calls run with the caller's scopes and permissions
//! under `mrsm start api` (Meerschaum's internal tables out of reach, `exec_sql` for callers
//! who may run actions, `host_scopes` for anonymous callers). SQLite instance, synthetic data, a
//! temporary root and plugins directory and a scrubbed environment.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;

const SOURCE: &str = r#"//! Host calls between two pipes.

/// `count` hourly rows from 2024-01-01.
fn hourly(count: i64) -> Vec<Value> {
    (0..count)
        .map(|i| json!({"ts": Bound::from_timestamp(1_704_067_200 + i * 3600).to_string(), "value": i}))
        .collect()
}

/// `parent` gets `rows` hourly rows (plugin config, default 3); any other metric rewrites its
/// window with the parent's values times `multiplier` (default 10): `mirror` in one transaction,
/// the others with a clear and a sync.
fn sync(pipe: &Pipe, begin: Option<Bound>, end: Option<Bound>) -> Result<SuccessTuple> {
    let config = host::plugin_config()?;
    if pipe.metric_key() == "parent" {
        return pipe.sync(hourly(config["rows"].as_i64().unwrap_or(3)));
    }
    let parent = host::pipe(pipe.connector_keys(), "parent", None, None)?;
    let multiplier = config["multiplier"].as_i64().unwrap_or(10);
    let counted = host::read_sql(
        pipe.instance_keys(),
        &format!("SELECT COUNT(*) AS n FROM \"{}\"", parent.target()),
    )?;
    let total = counted[0]["n"].as_i64().unwrap_or(-1);
    let rows: Vec<Value> = parent
        .get_data(begin.clone(), end.clone(), None)?
        .into_iter()
        .map(|mut row| {
            row["value"] = json!(row["value"].as_i64().unwrap_or(0) * multiplier);
            row
        })
        .collect();
    if pipe.metric_key() == "mirror" {
        return pipe.replace(rows, begin, end);
    }
    let cleared = pipe.clear(begin, end, None)?;
    if !cleared.success {
        return Ok(cleared);
    }
    let synced = pipe.sync(rows)?;
    Ok(SuccessTuple {
        success: synced.success,
        message: format!("parent has {total} rows; {}", synced.message),
    })
}

/// Parameters for new pipes, from this plugin's config.
fn register(pipe: &Pipe) -> Result<Value> {
    Ok(json!({"note": format!("{} rows={}", pipe.metric_key(), host::plugin_config()?["rows"])}))
}

/// Write the default config when there is none (`mrsm setup plugins hosted`).
fn setup() -> Result<SuccessTuple> {
    if host::plugin_config()?.is_null() {
        host::write_plugin_config(&json!({"rows": 3, "multiplier": 10}))?;
    }
    Ok(SuccessTuple::ok(format!("setup {}", host::plugin_config()?)))
}

/// Write this plugin's config.
#[make_action]
fn hosted_configure() -> Result<SuccessTuple> {
    let written = host::write_plugin_config(&json!({"rows": 4, "multiplier": 100}))?;
    Ok(SuccessTuple { success: written.success, message: format!("config {}", host::plugin_config()?) })
}

/// Empty the mirror's first two hours in one transaction.
#[make_action]
fn hosted_trim(kwargs: &Value) -> Result<SuccessTuple> {
    let instance = kwargs["mrsm_instance"].as_str().unwrap_or("sql:local");
    let mirror = host::pipe("plugin:hosted", "mirror", None, Some(instance))?;
    mirror.replace(
        Vec::<Value>::new(),
        Some(Bound::from("2024-01-01T00:00:00+00:00")),
        Some(Bound::from("2024-01-01T02:00:00+00:00")),
    )
}

/// Make the user `root` an administrator (a local action has the trust of the process).
#[make_action]
fn hosted_promote(kwargs: &Value) -> Result<SuccessTuple> {
    let instance = kwargs["mrsm_instance"].as_str().unwrap_or("sql:local");
    host::exec_sql(instance, "UPDATE mrsm_users SET user_type = 'admin' WHERE username = 'root'")
}

/// Report a pipe (`mrsm hosted check [metric]`, the child by default).
#[make_action]
fn hosted_check(action: Vec<String>, kwargs: &Value) -> Result<SuccessTuple> {
    let instance = kwargs["mrsm_instance"].as_str().unwrap_or("sql:local");
    let metric = action.first().map(String::as_str).unwrap_or("child");
    let child = host::pipe("plugin:hosted", metric, None, Some(instance))?;
    let values: Vec<i64> = child
        .get_data(None, None, None)?
        .iter()
        .map(|row| row["value"].as_i64().unwrap_or(-1))
        .collect();
    let latest = child.get_data_with(&Query::new().order("desc").limit(1))?;
    let marker = host::exec_sql(instance, "CREATE TABLE IF NOT EXISTS hosted_marker (id INTEGER)")?;
    let missing = host::pipe("plugin:hosted", "missing", None, Some(instance))?;
    Ok(SuccessTuple::ok(format!(
        "values={values:?} count={} latest={} sync_time={} exists={} missing={} exec={} columns={} note={}",
        child.get_rowcount(None, None, None)?,
        latest[0]["value"],
        child.get_sync_time()?.map(|b| b.to_string()).unwrap_or_default(),
        child.exists()?,
        missing.exists()?,
        marker.success,
        child.get_columns_types()?.len(),
        child.parameters()["note"],
    )))
}

/// The rows of a pipe.
#[api_route(GET, "/hosted/{metric}/rowcount", scopes = ["pipes:read"])]
fn rowcount(request: &RouteRequest) -> Result<Value> {
    let pipe = host::pipe("plugin:hosted", request.path_param("metric").unwrap_or(""), None, None)?;
    Ok(json!({"rowcount": pipe.get_rowcount(None, None, None)?, "instance": pipe.instance_keys()}))
}

/// A query on the instance: the host needs `sql:read` and a read-only query.
#[api_route(GET, "/hosted/sql")]
fn sql(request: &RouteRequest) -> RouteResponse {
    match host::read_sql("sql:local", request.query_param("q").unwrap_or("SELECT 1 AS one")) {
        Ok(rows) => RouteResponse::json(&json!(rows)),
        Err(error) => RouteResponse::error(403, error.to_string()),
    }
}

/// A statement on the instance: the host needs `actions:execute` and a caller who may run actions.
#[api_route(POST, "/hosted/exec")]
fn exec(request: &RouteRequest) -> RouteResponse {
    let statement = request
        .query_param("q")
        .unwrap_or("CREATE TABLE IF NOT EXISTS route_marker (id INTEGER)");
    match host::exec_sql("sql:local", statement) {
        Ok(done) => RouteResponse::json(&json!(done)),
        Err(error) => RouteResponse::error(403, error.to_string()),
    }
}

/// A public row count: its host calls may read pipes for anyone.
#[api_route(GET, "/hosted/public/{metric}/rowcount", public = true, host_scopes = ["pipes:read"])]
fn public_rowcount(request: &RouteRequest) -> RouteResponse {
    let metric = request.path_param("metric").unwrap_or("");
    match host::pipe("plugin:hosted", metric, None, None).and_then(|p| p.get_rowcount(None, None, None)) {
        Ok(rowcount) => RouteResponse::json(&json!({"rowcount": rowcount})),
        Err(error) => RouteResponse::error(403, error.to_string()),
    }
}

/// A public route without `host_scopes`: its host calls are refused to anonymous callers.
#[api_route(GET, "/hosted/public/{metric}/locked", public = true)]
fn public_locked(request: &RouteRequest) -> RouteResponse {
    let metric = request.path_param("metric").unwrap_or("");
    match host::pipe("plugin:hosted", metric, None, None).and_then(|p| p.get_rowcount(None, None, None)) {
        Ok(rowcount) => RouteResponse::json(&json!({"rowcount": rowcount})),
        Err(error) => RouteResponse::error(403, error.to_string()),
    }
}

/// The Meerschaum config: only for a caller with every scope.
#[api_route(GET, "/hosted/setting")]
fn setting(_request: &RouteRequest) -> RouteResponse {
    match host::config(&["meerschaum", "instance"]) {
        Ok(value) => RouteResponse::json(&value),
        Err(error) => RouteResponse::error(403, error.to_string()),
    }
}

/// Clear a pipe: the host refuses it to a caller without `pipes:write`.
#[api_route(POST, "/hosted/{metric}/clear")]
fn clear(request: &RouteRequest) -> RouteResponse {
    let metric = request.path_param("metric").unwrap_or("");
    match host::pipe("plugin:hosted", metric, None, None).and_then(|pipe| pipe.clear(None, None, None)) {
        Ok(cleared) => RouteResponse::json(&json!(cleared)),
        Err(error) => RouteResponse::error(403, error.to_string()),
    }
}
"#;

/// A post-sync hook that records every sync in a log pipe on another instance (`sql:other`), as
/// a telemetry plugin writes to a database of its own.
const CROSSED: &str = r#"//! A sync hook writing to another instance.

fn fetch() -> Vec<Value> {
    vec![json!({"id": 1})]
}

/// Log a sync of a pipe on any other instance than the log's.
#[post_sync_hook]
fn record(pipe: &Pipe) -> Result<SuccessTuple> {
    if pipe.instance_keys() == "sql:other" {
        return Ok(SuccessTuple::ok("not logged"));
    }
    host::pipe("plugin:crossed", "log", None, Some("sql:other"))?
        .sync(vec![json!({"id": 1, "metric": pipe.metric_key()})])
}

/// The log's rows (`mrsm crossed check`).
#[make_action]
fn crossed_check() -> Result<SuccessTuple> {
    let log = host::pipe("plugin:crossed", "log", None, Some("sql:other"))?;
    let rows = log.get_data(None, None, None)?;
    Ok(SuccessTuple::ok(format!(
        "log rows={} metric={}",
        log.get_rowcount(None, None, None)?,
        rows.first().map(|row| row["metric"].clone()).unwrap_or_default(),
    )))
}
"#;

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    /// A root with the plugin compiled into its build cache.
    fn new() -> Home {
        Home::with_plugin("hosted", SOURCE)
    }

    /// A root with the single-file plugin `name` compiled into its build cache.
    fn with_plugin(name: &str, source: &str) -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        };
        std::fs::write(home.plugins.path().join(format!("{name}.rs")), source).unwrap();
        let builds = home.root.path().join("cache/native_build");
        let found = meerschaum_plugin::discovery::discover_report_in(
            &[home.plugins.path().into()],
            &builds,
        );
        assert!(found.skipped.is_empty(), "{:?}", found.skipped);
        let candidate = &found.candidates[0];
        compile_single_file::compile(candidate);
        std::fs::write(
            builds.join(format!("{name}/mrsm-source-fingerprint")),
            &candidate.fingerprint,
        )
        .unwrap();
        home
    }

    /// `mrsm` in this root, with a second SQLite instance `sql:other` next to `sql:local`.
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env(
                "MRSM_SQL_OTHER",
                format!("sqlite:///{}", self.root.path().join("other.db").display()),
            )
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NOASK", "1");
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut child = self
            .command()
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let start = Instant::now();
        loop {
            if child.try_wait().unwrap().is_some() {
                return child.wait_with_output().unwrap();
            }
            if start.elapsed() > Duration::from_secs(120) {
                child.kill().unwrap();
                panic!("`mrsm {}` timed out", args.join(" "));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Run `mrsm <args> -i sql:local` and return its output, failing the test on an error.
    /// (Without `--nopretty`, which leaves out an action's message.)
    fn ok(&self, args: &[&str]) -> String {
        let mut all = args.to_vec();
        all.extend(["-i", "sql:local"]);
        let output = self.run(&all);
        let text = text(&output);
        assert!(output.status.success(), "mrsm {}: {text}", all.join(" "));
        text
    }

    fn register(&self, metric: &str) {
        self.ok(&[
            "register",
            "pipe",
            "-c",
            "plugin:hosted",
            "-m",
            metric,
            "--params",
            r#"{"columns": {"datetime": "ts"}, "dtypes": {"value": "int"}}"#,
        ]);
    }

    fn sync(&self, metric: &str, extra: &[&str]) -> String {
        let mut args = vec![
            "sync",
            "pipes",
            "-c",
            "plugin:hosted",
            "-m",
            metric,
            "--nopretty",
        ];
        args.extend(extra);
        self.ok(&args)
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
fn a_plugin_reads_a_parent_runs_sql_rewrites_a_window_and_uses_its_config() {
    let home = Home::new();
    // `setup` writes the plugin's default config through the host.
    home.ok(&["setup", "plugins", "hosted"]);
    let plugins_json = home.root.path().join("config/plugins.json");
    let config = |path: &Path| -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    };
    assert_eq!(
        config(&plugins_json),
        serde_json::json!({"hosted": {"rows": 3, "multiplier": 10}})
    );
    home.register("parent");
    home.register("child");
    let parent = home.sync("parent", &[]);
    assert!(parent.contains("Inserted 3"), "{parent}");

    // The window [01:00, 03:00) of the parent, times 10, after clearing the child's window.
    let child = home.sync(
        "child",
        &[
            "--begin",
            "2024-01-01T01:00:00",
            "--end",
            "2024-01-01T03:00:00",
        ],
    );
    assert!(child.contains("parent has 3 rows"), "{child}");
    let check = home.ok(&["hosted", "check"]);
    assert!(check.contains("values=[10, 20] count=2"), "{check}");
    // `register` made host calls too: its note comes from the plugin's config.
    assert!(check.contains(r#"note="child rows=3""#), "{check}");

    // The action writes the plugin's config through the host; the next syncs read it.
    let configured = home.ok(&["hosted", "configure"]);
    assert!(
        configured.contains(r#"config {"rows":4,"multiplier":100}"#),
        "{configured}"
    );
    // The `plugins` file of the config directory, as Python's `write_plugin_config()` writes it.
    assert_eq!(
        config(&plugins_json),
        serde_json::json!({"hosted": {"rows": 4, "multiplier": 100}})
    );
    let parent = home.sync("parent", &[]);
    assert!(parent.contains("Inserted 1"), "{parent}");
    // Unbounded: the whole child is cleared and rewritten, so a rerun changes nothing.
    for _ in 0..2 {
        let child = home.sync("child", &[]);
        assert!(child.contains("parent has 4 rows"), "{child}");
        let check = home.ok(&["hosted", "check"]);
        assert!(
            check.contains("values=[0, 100, 200, 300] count=4 latest=300"),
            "{check}"
        );
        assert!(check.contains("sync_time=2024-01-01T03:00:00"), "{check}");
        assert!(
            check.contains("exists=true missing=false exec=true columns=2"),
            "{check}"
        );
    }

    // `pipe.replace(rows, begin, end)` rewrites a window in one transaction.
    home.register("mirror");
    home.sync(
        "mirror",
        &[
            "--begin",
            "2024-01-01T01:00:00",
            "--end",
            "2024-01-01T03:00:00",
        ],
    );
    let check = home.ok(&["hosted", "check", "mirror"]);
    assert!(check.contains("values=[100, 200] count=2"), "{check}");
    assert!(check.contains(r#"note="mirror rows=4""#), "{check}");
    // No rows and both bounds: the window is emptied.
    home.ok(&["hosted", "trim"]);
    let check = home.ok(&["hosted", "check", "mirror"]);
    assert!(check.contains("values=[200] count=1"), "{check}");
    // Unbounded: the rows' own range is replaced, so a rerun changes nothing.
    for _ in 0..2 {
        home.sync("mirror", &[]);
        let check = home.ok(&["hosted", "check", "mirror"]);
        assert!(
            check.contains("values=[0, 100, 200, 300] count=4"),
            "{check}"
        );
    }
}

struct Server {
    child: Child,
    port: u16,
    stderr: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `mrsm start api` on a random loopback port, without the console.
fn serve(home: &Home) -> Server {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
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
    let deadline = Instant::now() + Duration::from_secs(60);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited: {}",
            std::fs::read_to_string(&server.stderr).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "server did not listen in time");
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

/// `(status, body)` of one HTTP/1.1 request.
fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
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
        let mut out = Vec::new();
        let mut data = body.as_slice();
        loop {
            let end = data.windows(2).position(|w| w == b"\r\n").unwrap();
            let size = usize::from_str_radix(std::str::from_utf8(&data[..end]).unwrap().trim(), 16)
                .unwrap();
            if size == 0 {
                break;
            }
            out.extend_from_slice(&data[end + 2..end + 2 + size]);
            data = &data[end + 2 + size + 2..];
        }
        body = out;
    }
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn register_user(home: &Home, username: &str) {
    let mut registering = home
        .command()
        .env_remove("MRSM_NOASK")
        .args([
            "register",
            "user",
            username,
            "-i",
            "sql:local",
            "--nopretty",
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
        .write_all(b"synthetic-pass-1\nsynthetic-pass-1\n\n")
        .unwrap();
    let output = registering.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", text(&output));
}

fn login(port: u16, username: &str, scope: &str) -> String {
    let form = format!("username={username}&password=synthetic-pass-1&scope={scope}");
    let (status, body) = http(
        port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    );
    assert_eq!(status, 200, "{body}");
    let token: serde_json::Value = serde_json::from_str(&body).unwrap();
    format!("Bearer {}", token["access_token"].as_str().unwrap())
}

#[test]
fn route_host_calls_run_with_the_callers_scopes() {
    let home = Home::new();
    home.register("parent");
    home.sync("parent", &[]);
    register_user(&home, "ada");
    let server = serve(&home);
    let port = server.port;
    let reader = login(port, "ada", "pipes:read");
    let (status, body) = http(
        port,
        "GET",
        "/hosted/parent/rowcount",
        &[("Authorization", &reader)],
        b"",
    );
    assert_eq!(status, 200, "{body}");
    let reply: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Without instance keys, a route's Pipe is on the server's instance.
    assert_eq!(
        reply,
        serde_json::json!({"rowcount": 3, "instance": "sql:local"})
    );

    // A reader may call the route, but the host refuses its clear.
    let (status, body) = http(
        port,
        "POST",
        "/hosted/parent/clear",
        &[("Authorization", &reader)],
        b"",
    );
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("`pipes:write`"), "{body}");
    let (_, body) = http(
        port,
        "GET",
        "/hosted/parent/rowcount",
        &[("Authorization", &reader)],
        b"",
    );
    assert!(body.contains(r#""rowcount":3"#), "{body}");

    let writer = login(port, "ada", "pipes:read+pipes:write");
    let (status, body) = http(
        port,
        "POST",
        "/hosted/parent/clear",
        &[("Authorization", &writer)],
        b"",
    );
    assert_eq!(status, 200, "{body}");
    assert!(body.starts_with("[true"), "{body}");
    let (_, body) = http(
        port,
        "GET",
        "/hosted/parent/rowcount",
        &[("Authorization", &writer)],
        b"",
    );
    assert!(body.contains(r#""rowcount":0"#), "{body}");

    // `read_sql` needs `sql:read`, and only read-only queries run for an API caller.
    let get = |path: &str, auth: &str| http(port, "GET", path, &[("Authorization", auth)], b"");
    let (status, body) = get("/hosted/sql", &reader);
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("`sql:read`"), "{body}");
    let analyst = login(port, "ada", "sql:read");
    let (status, body) = get("/hosted/sql", &analyst);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, r#"[{"one":1}]"#);
    let (status, body) = get(
        "/hosted/sql?q=DELETE%20FROM%20plugin_hosted_parent",
        &analyst,
    );
    assert_eq!(status, 403, "{body}");
    // The Meerschaum config is not for a caller with only some scopes.
    let (status, body) = get("/hosted/setting", &writer);
    assert_eq!(status, 403, "{body}");

    // Meerschaum's internal tables are never reachable from an API caller's SQL, quoted or not.
    for query in [
        "SELECT%20*%20FROM%20mrsm_users",
        "SELECT%20password_hash%20FROM%20%22mrsm_users%22",
        "SELECT%20*%20FROM%20mrsm_tokens",
    ] {
        let (status, body) = get(&format!("/hosted/sql?q={query}"), &analyst);
        assert_eq!(status, 403, "{body}");
        assert!(body.contains("internal table"), "{body}");
    }
    // `exec_sql` needs a caller who may run actions: not a user with `actions:execute` alone...
    let post = |path: &str, auth: &str| http(port, "POST", path, &[("Authorization", auth)], b"");
    let executor = login(port, "ada", "actions:execute");
    let (status, body) = post("/hosted/exec", &executor);
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("may not execute SQL statements"), "{body}");
    // ...but an administrator, and never on the internal tables.
    register_user(&home, "root");
    home.ok(&["hosted", "promote"]);
    let admin = login(port, "root", "actions:execute");
    let (status, body) = post("/hosted/exec", &admin);
    assert_eq!(status, 200, "{body}");
    assert!(body.starts_with("[true"), "{body}");
    let (status, body) = post(
        "/hosted/exec?q=UPDATE%20mrsm_users%20SET%20user_type%20%3D%20%27admin%27",
        &admin,
    );
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("internal table"), "{body}");

    // A public route's `host_scopes` let anonymous callers read; without them, nothing.
    let (status, body) = http(port, "GET", "/hosted/public/parent/rowcount", &[], b"");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, r#"{"rowcount":0}"#);
    let (status, body) = http(port, "GET", "/hosted/public/parent/locked", &[], b"");
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("`pipes:read`"), "{body}");
    drop(server);
    assert!(Path::new(&home.root.path().join("server.err")).exists());
}

/// A hook in an engine run syncs a pipe on an instance the run itself does not use: the host
/// reaches that instance through the process's connectors, as its reads do.
#[test]
fn a_sync_hook_writes_to_a_pipe_on_another_instance() {
    let home = Home::with_plugin("crossed", CROSSED);
    home.ok(&["register", "pipe", "-c", "plugin:crossed", "-m", "data"]);
    let synced = home.ok(&[
        "sync",
        "pipes",
        "-c",
        "plugin:crossed",
        "-m",
        "data",
        "--nopretty",
    ]);
    assert!(!synced.contains("No instance connector"), "{synced}");
    let check = home.ok(&["crossed", "check"]);
    assert!(check.contains(r#"log rows=1 metric="data""#), "{check}");
}
