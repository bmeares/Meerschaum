//! 5.1: team execution end to end. A member of an isolated team whose members may install
//! plugins (a team that "runs code") may run commands, but only inside the team's sandboxed
//! processes: `POST /execute` and `POST /jobs/{name}` start team jobs that require the sandbox,
//! and the web terminal is a team terminal (the `mrsm` shell as a team process, on the team's
//! own locked-down tmux server). Everyone else who is not an administrator keeps the
//! deny-by-default policy; administrators are unaffected; with the sandbox off the member is
//! refused with the reason.
//!
//! A real `start api` on a throwaway PostgreSQL instance (`MRSM_TEST_PG_URI`), a temporary root
//! and plugins directory, a scrubbed environment. Linux with Landlock ABI 9, tmux and a Python
//! `meerschaum` that applies team scoping (`MRSM_PYTHON`) are needed; the test says why it skips
//! otherwise. Every escape attempt writes its outcome as a file in the team's temporary
//! directory (`yes` / `no`), or a marker outside it that must not appear. The container socket
//! is only connected to with a read-only `GET /_ping`. All data is synthetic.
#![cfg(target_os = "linux")]

mod common;
use common::appears;
use common::webterm::plain;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::users::User;
use meerschaum_server::config::ServerConfig;
use meerschaum_server::state::AppState;
use meerschaum_web::sessions::SessionStore;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

const PASSWORD: &str = "synthetic-pass-1";
const SECRET: &str = "tpr-synthetic-secret-7f3a";
const CONFIG_PASSWORD: &str = "tpr-config-password-91c2";
const INSTANCE: &str = "sql:tpr";

const OPERATOR: &[&str] = &[
    "webterm:use",
    "actions:execute",
    "jobs:execute",
    "jobs:write",
    "jobs:read",
    "jobs:stop",
    "jobs:delete",
    "logs:read",
    "pipes:read",
    "pipes:write",
    "pipes:execute",
];

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
    marks: tempfile::TempDir,
    pg_uri: String,
    sandbox: &'static str,
}

impl Home {
    fn new(pg_uri: &str) -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
            marks: tempfile::tempdir().unwrap(),
            pg_uri: pg_uri.to_string(),
            sandbox: "auto",
        };
        let root = home.root.path();
        std::fs::create_dir_all(root.join("config")).unwrap();
        // A password in the server's own configuration, which no team process may read.
        let db = |name: &str| root.join(format!("{name}.db")).display().to_string();
        std::fs::write(
            root.join("config").join("meerschaum.yaml"),
            format!(
                "instance: {INSTANCE}\nconnectors:\n  sql:\n    main:\n      uri: '{pg_uri}'\n    tpr:\n      uri: '{pg_uri}'\n    unused:\n      flavor: sqlite\n      database: {unused}\n      password: {CONFIG_PASSWORD}\n    coders:\n      flavor: sqlite\n      database: {coders}\n      teams: [Coders]\n    readers:\n      flavor: sqlite\n      database: {readers}\n      teams: [Readers]\n",
                unused = db("unused"),
                coders = db("coders"),
                readers = db("readers"),
            ),
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                root.join("config").join("meerschaum.yaml"),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        std::fs::write(root.join("secret.txt"), SECRET).unwrap();
        home
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("SHELL", "/bin/sh")
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_NOASK", "true")
            // A variable of the server's own that no team process may see.
            .env("TPR_SERVER_SECRET", SECRET)
            .env(
                "MRSM_CONFIG",
                json!({
                    "formatting": {"ansi": false, "unicode": false},
                    "shell": {"clear_screen": false, "bottom_toolbar": {"enabled": false}},
                    "api": {"permissions": {"actions": {"non_admin": true}}},
                    "system": {
                        "api": {"permissions": {"actions": {"non_admin": true}}},
                        "engine": {"python_team_sandbox": self.sandbox},
                    },
                    "jobs": {"startup": {"enabled": false}},
                })
                .to_string(),
            );
        for name in ["MRSM_PYTHON", "PYTHONPATH", "LANG"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command
    }

    fn register_user(&self, username: &str, admin: bool, scopes: &[&str]) {
        let mut command = self.command();
        command.env_remove("MRSM_NOASK").args([
            "register",
            "user",
            username,
            "-i",
            INSTANCE,
            "--nopretty",
        ]);
        if !scopes.is_empty() {
            command.arg("--scopes").args(scopes);
        }
        let mut registering = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        registering
            .stdin
            .take()
            .unwrap()
            .write_all(
                format!(
                    "{}\n{PASSWORD}\n{PASSWORD}\n\n",
                    if admin { "y" } else { "n" }
                )
                .as_bytes(),
            )
            .unwrap();
        let output = registering.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    fn mark(&self, name: &str) -> PathBuf {
        self.marks.path().join(name)
    }

    fn server_config(&self) -> ServerConfig {
        ServerConfig::for_root(INSTANCE, self.root.path())
    }

    /// The team's scope key, once the server recorded it.
    fn scope_key(&self, team: &str) -> Option<String> {
        meerschaum_core::team_scope::scope_key_of(self.root.path(), team)
    }

    /// The team's temporary directory (its `TMPDIR`, where its processes may write).
    fn team_tmp(&self, team: &str) -> PathBuf {
        let key = self.scope_key(team).expect("the team's scope");
        meerschaum_core::team_scope::sandbox::team_tmp_dir(self.root.path(), &key).unwrap()
    }
}

struct Server {
    child: Child,
    port: u16,
    stderr: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn log(&self) -> String {
        std::fs::read_to_string(&self.stderr).unwrap_or_default()
    }
}

fn serve(home: &Home) -> Server {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert!(![3000, 5432, 6379, 8000, 8989].contains(&port));
    drop(socket);
    let stderr = home.root.path().join(format!("server-{port}.err"));
    let mut command = home.command();
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command
        .args(["start", "api", "-i", INSTANCE, "--host", "127.0.0.1", "-p"])
        .arg(port.to_string())
        .args(["-w", "2"])
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
    let deadline = Instant::now() + Duration::from_secs(120);
    while http(port, "GET", "/healthcheck/ready", &[], b"").map(|r| r.0) != Some(200) {
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited before it was ready: {}",
            server.log()
        );
        assert!(Instant::now() < deadline, "server did not get ready");
        std::thread::sleep(Duration::from_millis(100));
    }
    server
}

fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (key, value) in headers {
        head.push_str(&format!("{key}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body).ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let split = response.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&response[..split]).into_owned();
    let status = head.split(' ').nth(1)?.parse().ok()?;
    Some((
        status,
        String::from_utf8_lossy(&response[split + 4..]).into_owned(),
    ))
}

fn login(port: u16, username: &str) -> String {
    let form = format!("username={username}&password={PASSWORD}");
    let (_, body) = http(
        port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    )
    .expect("login");
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    format!(
        "Bearer {}",
        value["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("no token: {body}"))
    )
}

fn send(port: u16, method: &str, auth: &str, path: &str, body: Value) -> (u16, String) {
    http(
        port,
        method,
        path,
        &[
            ("Authorization", auth),
            ("Content-Type", "application/json"),
        ],
        body.to_string().as_bytes(),
    )
    .unwrap_or_else(|| panic!("no reply from {path}"))
}

/// A shell script that tries every way out and records each outcome under `$TMPDIR` as
/// `<prefix>_<what>` (`yes`: it worked, `no`: refused), then `<prefix>_done`.
fn escape_script(home: &Home, prefix: &str, server_pid: u32, other_job: &Path) -> String {
    let root = home.root.path().display();
    let other_job = other_job.display();
    let outside = home
        .mark(&format!("{prefix}_outside"))
        .display()
        .to_string();
    format!(
        "t=\"$TMPDIR\"; \
         echo ran > \"$t/{prefix}_marker\"; \
         try() {{ if sh -c \"$2\" >/dev/null 2>&1; then echo yes > \"$t/{prefix}_$1\"; else echo no > \"$t/{prefix}_$1\"; fi; }}; \
         try config \"grep -q {CONFIG_PASSWORD} '{root}/config/meerschaum.yaml'\"; \
         try secret \"grep -q {SECRET} '{root}/secret.txt'\"; \
         try other_job \"ls '{other_job}'\"; \
         try environ \"grep -q TPR_SERVER_SECRET /proc/{server_pid}/environ\"; \
         try own_env \"env | grep -q TPR_SERVER_SECRET\"; \
         try outside \"touch '{outside}'\"; \
         try signal \"kill -0 {server_pid}\"; \
         try docker \"curl -s -m 5 --unix-socket /var/run/docker.sock http://localhost/_ping\"; \
         echo done > \"$t/{prefix}_done\""
    )
}

/// The outcomes [`escape_script`] recorded: every way out refused, the marker written.
fn assert_contained(home: &Home, team: &str, prefix: &str) {
    let tmp = home.team_tmp(team);
    assert!(
        appears(&tmp.join(format!("{prefix}_done")), Duration::from_secs(90)),
        "{prefix}: the command did not finish\n{}",
        jobs_dump(home)
    );
    assert!(tmp.join(format!("{prefix}_marker")).is_file(), "{prefix}");
    for what in [
        "config",
        "secret",
        "other_job",
        "environ",
        "own_env",
        "outside",
        "signal",
        "docker",
    ] {
        let outcome =
            std::fs::read_to_string(tmp.join(format!("{prefix}_{what}"))).unwrap_or_default();
        assert_eq!(outcome.trim(), "no", "{prefix}: `{what}` was not refused");
    }
    assert!(
        !home.mark(&format!("{prefix}_outside")).exists(),
        "{prefix}"
    );
}

/// Every job's properties and log, for a failure message.
fn jobs_dump(home: &Home) -> String {
    let mut out = String::new();
    let jobs = home.root.path().join("jobs");
    for entry in std::fs::read_dir(&jobs).into_iter().flatten().flatten() {
        out.push_str(&format!("== {}\n", entry.path().display()));
        for file in std::fs::read_dir(entry.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = file.file_name().to_string_lossy().into_owned();
            if name.ends_with(".log") || name == "properties.json" || name.ends_with(".json") {
                out.push_str(&format!(
                    "-- {name}\n{}\n",
                    std::fs::read_to_string(file.path()).unwrap_or_default()
                ));
            }
        }
    }
    let logs = home.root.path().join("logs");
    for entry in std::fs::read_dir(&logs).into_iter().flatten().flatten() {
        out.push_str(&format!(
            "-- {}\n{}\n",
            entry.path().display(),
            std::fs::read_to_string(entry.path()).unwrap_or_default()
        ));
    }
    out
}

/// The server's instance and state, in this process (the same database and secret key), to
/// write console sessions as the console does.
struct Instance {
    state: AppState,
}

impl Instance {
    async fn open(home: &Home) -> Instance {
        let conn = Arc::new(SqlConnector::from_uri("tpr", &home.pg_uri).await.unwrap());
        let instance: Arc<dyn InstanceConnector> = conn;
        let state = AppState::builder(home.server_config(), instance)
            .build()
            .unwrap();
        Instance { state }
    }

    async fn session(&self, username: &str) -> String {
        let user = User::new(username, None);
        let credential = meerschaum_server::auth::user_credential(&self.state, &user)
            .await
            .unwrap()
            .expect("a credential");
        let id = uuid::Uuid::new_v4().to_string();
        meerschaum_web::sessions::InstanceSessions::new(self.state.instance())
            .set(
                &id,
                json!({"username": username, "credential": credential}),
                3600,
            )
            .await
            .unwrap();
        id
    }
}

struct Term {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    out: String,
    closed: bool,
}

impl Term {
    async fn open(port: u16, session: &str) -> Term {
        let (socket, _) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/websocket/{session}"))
                .await
                .unwrap();
        let mut term = Term {
            socket,
            out: String::new(),
            closed: false,
        };
        term.wait_for(
            |t| t.contains("[\"setup\"") || t.contains("[close"),
            Duration::from_secs(60),
        )
        .await;
        term.send_json(json!(["set_size", 40, 160, 800, 1600]))
            .await;
        term
    }

    async fn send_json(&mut self, value: Value) {
        let _ = self
            .socket
            .send(Message::Text(value.to_string().into()))
            .await;
    }

    async fn send(&mut self, text: &str) {
        self.send_json(json!(["stdin", text])).await;
    }

    async fn line(&mut self, line: &str) {
        self.send(line).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.send("\r").await;
    }

    async fn wait_for(&mut self, done: impl Fn(&str) -> bool, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if done(&self.out) {
                return true;
            }
            if self.closed {
                return false;
            }
            let Ok(message) = tokio::time::timeout_at(deadline, self.socket.next()).await else {
                return done(&self.out);
            };
            match message {
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(text.as_str()).unwrap_or(Value::Null);
                    match value[0].as_str() {
                        Some("stdout") => {
                            self.out.push_str(&plain(value[1].as_str().unwrap_or("")))
                        }
                        _ => self.out.push_str(text.as_str()),
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    self.out.push_str(&format!("[close {frame:?}]"));
                    self.closed = true;
                }
                Some(Ok(_)) => {}
                None | Some(Err(_)) => self.closed = true,
            }
        }
    }

    async fn drain(&mut self, duration: Duration) {
        let _ = self.wait_for(|_| false, duration).await;
    }

    async fn ready(&mut self) {
        let before = self.out.len();
        self.line("show version").await;
        assert!(
            self.wait_for(
                |out| out[before.min(out.len())..].contains("Meerschaum v"),
                Duration::from_secs(120)
            )
            .await,
            "the shell did not answer: {}",
            self.out
        );
    }
}

fn tmux(socket: &Path, args: &[&str]) -> std::process::Output {
    Command::new("tmux")
        .arg("-S")
        .arg(socket)
        .args(args)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

/// The team tmux server sockets under the root's webterm directory (`*-t-*.sock`).
fn team_sockets(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if path.is_dir() {
                walk(&path, out);
            } else if name.contains("-t-") && name.ends_with(".sock") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}

/// Every process running in a pane of the team's tmux server was started as a team process
/// (its environment names the team's sandbox spec); a pane with nothing running is fine.
fn panes_are_team_processes(socket: &Path) -> Result<usize, String> {
    let listing = tmux(
        socket,
        &["list-panes", "-a", "-F", "#{pane_pid} #{pane_dead}"],
    );
    let mut count = 0;
    for line in String::from_utf8_lossy(&listing.stdout).lines() {
        let mut words = line.split_whitespace();
        let (Some(pid), Some(dead)) = (words.next(), words.next()) else {
            continue;
        };
        if dead == "1" {
            continue;
        }
        let environ = std::fs::read(format!("/proc/{pid}/environ")).unwrap_or_default();
        let environ = String::from_utf8_lossy(&environ);
        if !environ.contains("MRSM_TEAM_SANDBOX=") || environ.contains("TPR_SERVER_SECRET") {
            return Err(format!("pane process {pid} is not a team process"));
        }
        count += 1;
    }
    Ok(count)
}

/// What a run leaves outside its temporary directories, removed however the test ends: the
/// processes started under its root (job supervisors), its database and the teams' temporary
/// directories.
struct Cleanup {
    root: PathBuf,
    pg_uri: String,
    database: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let root = self.root.display().to_string();
        for entry in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let cmdline = std::fs::read(entry.path().join("cmdline")).unwrap_or_default();
            if pid != std::process::id() && String::from_utf8_lossy(&cmdline).contains(&root) {
                let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            }
        }
        for team in ["Coders", "Readers"] {
            if let Some(key) = meerschaum_core::team_scope::scope_key_of(&self.root, team)
                && let Ok(dir) =
                    meerschaum_core::team_scope::sandbox::team_tmp_dir(&self.root, &key)
            {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
        let (uri, database) = (self.pg_uri.clone(), self.database.clone());
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Ok(conn) = SqlConnector::from_uri("tpr_cleanup", &uri).await {
                    let _ = conn
                        .driver()
                        .exec(&format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
                        .await;
                }
            })
        });
    }
}

fn skip(reason: &str) {
    eprintln!("SKIP team_execution: {reason}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_code_running_teams_commands_run_only_inside_its_sandbox() {
    let pg_uri = match std::env::var("MRSM_TEST_PG_URI") {
        Ok(uri) if !uri.is_empty() => uri,
        _ => return skip("set MRSM_TEST_PG_URI to a throwaway PostgreSQL"),
    };
    let support = meerschaum_core::team_scope::sandbox::kernel_support();
    if support.abi() < 9 {
        return skip(&format!("needs Landlock ABI 9 ({})", support.describe()));
    }
    if !Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .is_ok_and(|o| o.status.success())
    {
        return skip("no tmux");
    }
    if std::env::var_os("MRSM_PYTHON").is_none() {
        return skip("set MRSM_PYTHON to a Python with this workspace's `meerschaum`");
    }

    // A fresh database on the throwaway server for this run (dropped at the end).
    let admin_conn = SqlConnector::from_uri("tpr_admin", &pg_uri).await.unwrap();
    let database = format!("tpr_{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let created = admin_conn
        .driver()
        .exec(&format!("CREATE DATABASE {database}"))
        .await;
    assert!(created.is_ok(), "{created:?}");
    let admin_uri = pg_uri.clone();
    let pg_uri = format!("{}/{database}", pg_uri.rsplit_once('/').unwrap().0);
    let mut home = Home::new(&pg_uri);
    let _cleanup = Cleanup {
        root: home.root.path().to_path_buf(),
        pg_uri: admin_uri,
        database: database.clone(),
    };
    home.register_user("root", true, &[]);
    for user in ["carol", "rita", "oscar"] {
        home.register_user(user, false, OPERATOR);
    }
    let server = serve(&home);
    let port = server.port;
    let server_pid = server.child.id();
    let admin = login(port, "root");
    for (team, member, runs_code) in [("Coders", "carol", true), ("Readers", "rita", false)] {
        let (status, body) = send(
            port,
            "POST",
            &admin,
            &format!("/teams/{team}/register"),
            json!({
                "members": [member],
                "adopt": true,
                "attributes": {
                    "isolated": true,
                    "plugins_write": runs_code,
                    "instance": format!("sql:{}", team.to_lowercase()),
                },
            }),
        );
        assert_eq!(status, 200, "{body}");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while (home.scope_key("Coders").is_none() || home.scope_key("Readers").is_none())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(home.scope_key("Coders").is_some(), "{}", server.log());
    let carol = login(port, "carol");
    let rita = login(port, "rita");
    let oscar = login(port, "oscar");

    // A job directory of Readers', which Coders' processes must not read.
    let other_job = home.root.path().join("jobs").join("tpr-readers-job");
    std::fs::create_dir_all(&other_job).unwrap();
    std::fs::write(
        other_job.join("properties.json"),
        json!({"team": "Readers", "owner": "rita"}).to_string(),
    )
    .unwrap();

    // The team's member: the server answers "yes, as team execution".
    let mut validated = (0, String::new());
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        validated = send(
            port,
            "POST",
            &carol,
            "/execute/validate",
            json!({"sysargs": ["sh", "-c", "true"]}),
        );
        if validated.0 == 200 {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(validated.0, 200, "{}\n{}", validated.1, server.log());
    assert!(
        validated.1.contains("\"team_execution\":true"),
        "{}",
        validated.1
    );

    // POST /execute: `sh` and `!command`, as team jobs.
    let script = escape_script(&home, "exec", server_pid, &other_job);
    let (status, body) = send(
        port,
        "POST",
        &carol,
        "/execute",
        json!({"sysargs": ["sh", "eval", script]}),
    );
    assert_eq!(status, 200, "{body}");
    assert_contained(&home, "Coders", "exec");
    let bang = format!(
        "echo bang > \"$TMPDIR/bang\"; cat {}/secret.txt > \"$TMPDIR/bang_secret\"",
        home.root.path().display()
    );
    let (status, body) = send(
        port,
        "POST",
        &carol,
        "/execute",
        json!({"sysargs": ["!eval", bang]}),
    );
    assert_eq!(status, 200, "{body}");
    let tmp = home.team_tmp("Coders");
    assert!(appears(&tmp.join("bang"), Duration::from_secs(60)));
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !std::fs::read_to_string(tmp.join("bang_secret"))
            .unwrap_or_default()
            .contains(SECRET)
    );

    // POST /jobs/{name}: a team job; the job's `cwd` and log path are the server's.
    let script = escape_script(&home, "jobs", server_pid, &other_job);
    let log_path = home.mark("job_log");
    let (status, body) = send(
        port,
        "POST",
        &carol,
        "/jobs/tpr-carol-job",
        json!({
            "sysargs": ["sh", "eval", script],
            "properties": {"cwd": "/", "logs": {"path": log_path}, "team_scope": "other"},
        }),
    );
    assert_eq!(status, 200, "{body}");
    assert_contained(&home, "Coders", "jobs");
    assert!(!log_path.exists());
    let properties: Value = serde_json::from_str(
        &std::fs::read_to_string(home.root.path().join("jobs/tpr-carol-job/properties.json"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        properties["mrsm_team_execution"],
        json!(true),
        "{properties}"
    );
    assert_eq!(
        properties["team_scope"].as_str(),
        home.scope_key("Coders").as_deref()
    );

    // `/actions` and MCP run in the server's process: refused, pointing at `POST /execute`.
    let (status, body) = send(
        port,
        "POST",
        &carol,
        "/actions/sh",
        json!({"action": ["-c", "true"]}),
    );
    assert!(
        body.contains("POST /execute") || status == 403,
        "{status} {body}"
    );

    // The other isolated team and the open side: refused exactly as before.
    // Rita's team withholds `actions:execute` (as before 5.1); Oscar holds it and meets the
    // deny-by-default policy.
    for (who, auth, why) in [
        ("rita", &rita, "actions:execute"),
        ("oscar", &oscar, "cannot be run by non-administrators"),
    ] {
        let marker = home.mark(&format!("{who}_sh"));
        let (status, body) = send(
            port,
            "POST",
            auth,
            "/execute",
            json!({"sysargs": ["sh", "touch", marker.display().to_string()]}),
        );
        assert_eq!(status, 403, "{who}: {body}");
        assert!(body.contains(why), "{who}: {body}");
        assert!(!appears(&marker, Duration::from_secs(2)), "{who}");
    }
    // The administrator, unaffected.
    let admin_marker = home.mark("admin_sh");
    let (status, body) = send(
        port,
        "POST",
        &admin,
        "/execute",
        json!({"sysargs": ["sh", "touch", admin_marker.display().to_string()]}),
    );
    assert_eq!(status, 200, "{body}");
    assert!(appears(&admin_marker, Duration::from_secs(60)));

    // The web terminal's real transport.
    let instance = Instance::open(&home).await;
    let carol_session = instance.session("carol").await;
    let mut term = Term::open(port, &carol_session).await;
    term.ready().await;
    let script = escape_script(&home, "term", server_pid, &other_job);
    term.line(&format!("sh eval '{}'", script.replace('\'', "'\\''")))
        .await;
    assert_contained(&home, "Coders", "term");
    term.line("!eval 'echo bang > \"$TMPDIR/term_bang\"'").await;
    assert!(appears(&tmp.join("term_bang"), Duration::from_secs(30)));
    let sockets = team_sockets(home.root.path());
    assert_eq!(sockets.len(), 1, "one team tmux server: {sockets:?}");
    let team_socket = sockets[0].clone();
    assert_eq!(panes_are_team_processes(&team_socket), Ok(1));

    // tmux from inside the sandbox: connecting to the team's tmux server is refused, whatever the
    // command (a new window, a respawned pane, the command prompt, run-shell).
    for (name, args) in [
        ("tmux_new_window", "new-window"),
        ("tmux_respawn", "respawn-pane -k"),
        ("tmux_prompt", "command-prompt -I x"),
        ("tmux_run_shell", "run-shell"),
    ] {
        let escape = home.mark(name);
        term.line(&format!(
            "!eval 'tmux -S {} {args} \"touch {}\"; tmux {args} \"touch {}\"; echo done > \"$TMPDIR/{name}\"'",
            team_socket.display(),
            escape.display(),
            escape.display(),
        ))
        .await;
        assert!(appears(&tmp.join(name), Duration::from_secs(30)), "{name}");
        assert!(!appears(&escape, Duration::from_secs(1)), "{name} escaped");
    }
    // tmux's own keys (prefix, command prompt) reach nothing.
    let key_escape = home.mark("tmux_keys");
    for keys in ["\x02c", "\x02:", "\x00c"] {
        term.send(keys).await;
        term.line(&format!("run-shell 'touch {}'", key_escape.display()))
            .await;
    }
    assert!(!appears(&key_escape, Duration::from_secs(2)));
    // The console's new window runs the team's shell.
    term.send_json(json!(["new_window"])).await;
    term.drain(Duration::from_secs(3)).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && panes_are_team_processes(&team_socket) != Ok(2) {
        std::thread::sleep(Duration::from_millis(250));
    }
    assert_eq!(panes_are_team_processes(&team_socket), Ok(2));
    term.ready().await;
    term.line("!eval 'grep -q tpr-synthetic-secret /proc/*/environ 2>/dev/null && echo yes > \"$TMPDIR/nw_environ\" || echo no > \"$TMPDIR/nw_environ\"'")
        .await;
    assert!(appears(&tmp.join("nw_environ"), Duration::from_secs(30)));
    assert_eq!(
        std::fs::read_to_string(tmp.join("nw_environ"))
            .unwrap()
            .trim(),
        "no"
    );
    // A respawned pane (asked of the tmux server from outside) and a crashed shell run the
    // team's shell or nothing.
    let _ = tmux(&team_socket, &["respawn-pane", "-k"]);
    std::thread::sleep(Duration::from_secs(2));
    assert!(panes_are_team_processes(&team_socket).is_ok());
    term.line("!eval 'kill -9 $PPID'").await;
    std::thread::sleep(Duration::from_secs(2));
    assert!(panes_are_team_processes(&team_socket).is_ok());
    drop(term);

    // The other isolated team's member gets no terminal; the open-side operator gets the
    // restricted one, as before.
    let rita_session = instance.session("rita").await;
    let mut rita_term = Term::open(port, &rita_session).await;
    rita_term.drain(Duration::from_secs(3)).await;
    assert!(
        rita_term.out.contains("permission to use the terminal"),
        "{}",
        rita_term.out
    );
    let oscar_session = instance.session("oscar").await;
    let mut oscar_term = Term::open(port, &oscar_session).await;
    oscar_term.ready().await;
    let oscar_marker = home.mark("oscar_term");
    oscar_term
        .line(&format!("!touch {}", oscar_marker.display()))
        .await;
    oscar_term.drain(Duration::from_secs(3)).await;
    assert!(!oscar_marker.exists(), "{}", oscar_term.out);
    drop(oscar_term);
    // The administrator's terminal still runs anything.
    let admin_session = instance.session("root").await;
    let mut admin_term = Term::open(port, &admin_session).await;
    admin_term.ready().await;
    let admin_term_marker = home.mark("admin_term");
    admin_term
        .line(&format!("!touch {}", admin_term_marker.display()))
        .await;
    assert!(appears(&admin_term_marker, Duration::from_secs(30)));
    drop(admin_term);

    // Turning the team's switch off ends its terminals and refuses its commands.
    let carol_session = instance.session("carol").await;
    let mut term = Term::open(port, &carol_session).await;
    term.ready().await;
    let (status, body) = send(
        port,
        "PATCH",
        &admin,
        "/teams/Coders/edit",
        json!({"attributes": {"isolated": true, "plugins_write": false, "instance": "sql:coders"}}),
    );
    assert_eq!(status, 200, "{body}");
    assert!(
        term.wait_for(|out| out.contains("[close"), Duration::from_secs(60))
            .await,
        "the team terminal stayed open: {}",
        term.out
    );
    let carol = login(port, "carol");
    let (status, body) = send(
        port,
        "POST",
        &carol,
        "/execute/validate",
        json!({"sysargs": ["sh", "-c", "true"]}),
    );
    assert_eq!(status, 403, "{body}");
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline
        && team_sockets(home.root.path())
            .iter()
            .any(|s| tmux(s, &["list-sessions"]).status.success())
    {
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        !team_sockets(home.root.path())
            .iter()
            .any(|s| tmux(s, &["list-sessions"]).status.success()),
        "the team's tmux server outlived the switch"
    );
    let (status, body) = send(
        port,
        "PATCH",
        &admin,
        "/teams/Coders/edit",
        json!({"attributes": {"isolated": true, "plugins_write": true, "instance": "sql:coders"}}),
    );
    assert_eq!(status, 200, "{body}");

    // The sandbox off: the member is refused with the reason, the administrator is not.
    drop(server);
    home.sandbox = "off";
    let server = serve(&home);
    let port = server.port;
    let carol = login(port, "carol");
    let (status, body) = send(
        port,
        "POST",
        &carol,
        "/execute",
        json!({"sysargs": ["sh", "-c", "true"]}),
    );
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("sandbox is off"), "{body}");
    let mut term = Term::open(port, &instance.session("carol").await).await;
    term.drain(Duration::from_secs(5)).await;
    assert!(term.out.contains("sandbox is off"), "{}", term.out);
    let admin = login(port, "root");
    let admin_marker = home.mark("admin_sh_off");
    let (status, body) = send(
        port,
        "POST",
        &admin,
        "/execute",
        json!({"sysargs": ["sh", "touch", admin_marker.display().to_string()]}),
    );
    assert_eq!(status, 200, "{body}");
    assert!(appears(&admin_marker, Duration::from_secs(60)));
    drop(server);

    drop(instance);
    drop(admin_conn);
}
