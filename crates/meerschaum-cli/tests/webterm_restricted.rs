//! 5.1: a user who is not an administrator never runs a system command or arbitrary code through
//! the web terminal. A real `start api` serves the terminal; each test opens terminals through
//! its real transport (the websocket of `/websocket/{session}`, console sessions in the
//! instance's session store) as a non-administrator holding `webterm:use` and as an
//! administrator, and tries every way out it knows with a harmless marker: a file in a temporary
//! directory that must not appear for the non-administrator and must appear for the administrator
//! where the command is legitimate. The tmux escapes (the prefix key, the command prompt,
//! `new-window`, `run-shell`, `respawn-pane`, the console's new-window message) are included,
//! and so is a demotion while a terminal is open.
//!
//! Unix only, and only where `tmux` is installed (it says so and returns otherwise). A
//! temporary root and plugins directory and a scrubbed environment; all data is synthetic.
#![cfg(unix)]

mod common;
use common::appears;
use common::process::tmux_available;
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

/// The permissions of the non-administrator: the terminal and everything the action routes
/// offer.
const BOB_SCOPES: &[&str] = &[
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
    editor: PathBuf,
}

impl Home {
    fn new() -> Home {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let marks = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("sqlite")).unwrap();
        // `$EDITOR`: a program that leaves a marker, as an editor's shell escape would.
        let editor = marks.path().join("editor.sh");
        std::fs::write(
            &editor,
            format!(
                "#!/bin/sh\ntouch '{}'\n",
                marks.path().join("editor").display()
            ),
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&editor, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        Home {
            root,
            plugins,
            marks,
            editor,
        }
    }

    fn config(callback: Option<u16>) -> Value {
        let mut config = json!({
            "meerschaum": {"instance": "sql:local"},
            "formatting": {"ansi": false, "unicode": false},
            "shell": {"clear_screen": false, "bottom_toolbar": {"enabled": false}},
            "api": {"permissions": {"actions": {"non_admin": true}}},
            "system": {"api": {"permissions": {"actions": {"non_admin": true}}}},
            "jobs": {"startup": {"enabled": false}},
        });
        if let Some(port) = callback {
            config["api"]["execution"] =
                json!({"callback_url": format!("http://127.0.0.1:{port}")});
        }
        config
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("SHELL", "/bin/sh")
            .env("EDITOR", &self.editor)
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_NOASK", "true")
            .env("MRSM_CONFIG", Self::config(None).to_string());
        if let Some(temp) = std::env::var_os("TMPDIR") {
            command.env("TMPDIR", temp);
        }
        if let Some(python) = std::env::var_os("MRSM_PYTHON") {
            command.env("MRSM_PYTHON", python);
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
            "sql:local",
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

    /// Run `mrsm <args>` as the root's own CLI (the instance's credentials).
    fn mrsm(&self, args: &[&str]) -> std::process::Output {
        self.command()
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn mark(&self, name: &str) -> PathBuf {
        self.marks.path().join(name)
    }

    fn mark_str(&self, name: &str) -> String {
        self.mark(name).to_string_lossy().into_owned()
    }

    fn server_config(&self) -> ServerConfig {
        ServerConfig::for_root("sql:local", self.root.path())
    }

    fn database(&self) -> PathBuf {
        find(self.root.path(), "mrsm_local.db").expect("the instance's SQLite file")
    }
}

fn find(dir: &Path, name: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.file_name().is_some_and(|n| n == name) {
            return Some(path);
        }
        if path.is_dir()
            && let Some(found) = find(&path, name)
        {
            return Some(found);
        }
    }
    None
}

/// Stops and deletes a job of the test's root, however the test ends.
struct JobGuard<'a>(&'a Home, &'static str);

impl Drop for JobGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.mrsm(&["stop", "jobs", self.1, "-y"]);
        let _ = self.0.mrsm(&["delete", "jobs", self.1, "-y", "-f"]);
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
    let stderr = home.root.path().join("server.err");
    let mut command = home.command();
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    // The console's own routes (the terminal) are served without `--no-dash`.
    let child = command
        .env("MRSM_CONFIG", Home::config(Some(port)).to_string())
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
        .set_read_timeout(Some(Duration::from_secs(30)))
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

/// The server's instance and state, in this process (the same SQLite file and secret key), to
/// write console sessions as the console does and to change a user's role.
struct Instance {
    conn: Arc<SqlConnector>,
    state: AppState,
}

impl Instance {
    async fn open(home: &Home) -> Instance {
        let conn = Arc::new(
            SqlConnector::from_uri("local", &format!("sqlite:///{}", home.database().display()))
                .await
                .unwrap(),
        );
        let instance: Arc<dyn InstanceConnector> = conn.clone();
        let state = AppState::builder(home.server_config(), instance)
            .build()
            .unwrap();
        Instance { conn, state }
    }

    /// A console session of `username`, bound to their current credential (what the console's
    /// login stores).
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

    async fn set_user_type(&self, username: &str, user_type: &str) {
        let mut user = User::new(username, None);
        user.user_type = Some(user_type.into());
        let result = self.conn.edit_user(&user).await;
        assert!(result.success, "{}", result.message);
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// One terminal: the websocket and everything it printed (escape sequences removed).
struct Term {
    socket: Socket,
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
        assert!(
            term.wait_for(|t| t.contains("[\"setup\""), Duration::from_secs(30))
                .await,
            "no setup: {}",
            term.out
        );
        term.send_json(json!(["set_size", 40, 160, 800, 1600]))
            .await;
        term
    }

    async fn send_json(&mut self, value: Value) {
        self.socket
            .send(Message::Text(value.to_string().into()))
            .await
            .expect("send the terminal input");
    }

    async fn send(&mut self, text: &str) {
        self.send_json(json!(["stdin", text])).await;
    }

    /// Type `line` and Enter.
    async fn line(&mut self, line: &str) {
        self.send(line).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        self.send("\r").await;
    }

    /// Read until `done` holds for the raw transcript (messages as JSON) or the plain output.
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

    /// Read for `duration`.
    async fn drain(&mut self, duration: Duration) {
        let _ = self.wait_for(|_| false, duration).await;
    }

    /// Wait until the shell answers a command (`show version` prints the version).
    async fn ready(&mut self) {
        let before = self.out.len();
        self.line("show version").await;
        assert!(
            self.wait_for(
                |out| prompt_follows(&out[before..], |response| response.contains("Meerschaum v")),
                Duration::from_secs(90)
            )
            .await,
            "the shell did not answer: {}",
            self.out
        );
    }
}

/// A command finishes when its response is visible and the line editor is ready again.
fn prompt_follows(text: &str, response: impl FnOnce(&str) -> bool) -> bool {
    [" ] ➤", " ] >"]
        .into_iter()
        .filter_map(|prompt| text.rfind(prompt))
        .max()
        .is_some_and(|start| response(&text[..start]))
}

/// Whether terminal output says a line was refused (by the server's decision, or because the
/// terminal starts no other program).
fn refused(text: &str) -> bool {
    text.contains("non-administrators")
        || text.contains("Non-administrators")
        || text.contains("not a Meerschaum action")
        || text.contains("does not start other programs")
        || text.contains("does not open an editor")
        || text.contains("not among the commands non-administrators may run")
}

/// `tmux -S <socket> <args>` (never the default server, never the caller's tmux).
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

fn sessions_on(socket: &Path) -> Vec<String> {
    String::from_utf8_lossy(&tmux(socket, &["list-sessions", "-F", "#{session_name}"]).stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_administrators_terminal_runs_only_what_the_action_routes_allow() {
    if !tmux_available() {
        eprintln!(
            "SKIP a_non_administrators_terminal_runs_only_what_the_action_routes_allow: no tmux"
        );
        return;
    }
    let home = Home::new();
    home.register_user("root", true, &[]);
    home.register_user("bob", false, BOB_SCOPES);
    // A pipe for the editor and a job that waits for input, made by the administrator's CLI.
    let registered = home.mrsm(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "tpr_edit",
        "-i",
        "sql:local",
    ]);
    assert!(registered.status.success(), "{registered:?}");
    let stdin_mark = home.mark_str("job-input");
    // It gives up waiting after a while, so a failed test leaves nothing running.
    let job = home.mrsm(&[
        "sh",
        "read",
        "-t",
        "120",
        "line",
        "&&",
        "touch",
        &stdin_mark,
        "-d",
        "--name",
        "tpr-stdin",
    ]);
    assert!(job.status.success(), "{job:?}");
    let _job = JobGuard(&home, "tpr-stdin");

    let server = serve(&home);
    let port = server.port;
    let instance = Instance::open(&home).await;
    let config = home.server_config();
    let admin_socket = config.webterm_resources_path.join("tmux.sock");
    let restricted_socket = config.webterm_resources_path.join("tmux-r.sock");

    // An administrator's console run: a job only its creator and administrators see.
    let form = format!("username=root&password={PASSWORD}");
    let (_, login) = http(
        port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    )
    .unwrap();
    let root_token = serde_json::from_str::<Value>(&login).unwrap()["access_token"]
        .as_str()
        .unwrap()
        .to_string();
    let body = json!({"sysargs": ["show", "version"], "name": "execute-tpr-hidden"}).to_string();
    let (status, text) = http(
        port,
        "POST",
        "/execute",
        &[
            ("Authorization", &format!("Bearer {root_token}")),
            ("Content-Type", "application/json"),
        ],
        body.as_bytes(),
    )
    .unwrap();
    assert_eq!(status, 200, "{text}");
    let hidden_job = home.root.path().join("jobs").join("execute-tpr-hidden");

    // ---- The non-administrator. ----
    let bob_session = instance.session("bob").await;
    let mut bob = Term::open(port, &bob_session).await;
    bob.ready().await;
    // Allowed: what `POST /execute` allows bob (a Meerschaum action).
    let before = bob.out.len();
    bob.line("show pipes").await;
    assert!(
        bob.wait_for(
            |out| prompt_follows(&out[before..], |response| response.contains("tpr_edit")),
            Duration::from_secs(60)
        )
        .await,
        "{}",
        bob.out
    );
    // The restricted session lives on its own tmux server, with no key bindings.
    let restricted_sessions = sessions_on(&restricted_socket);
    assert!(
        matches!(restricted_sessions.as_slice(), [name] if name.starts_with("host-api-terminal-")),
        "{:?} {}",
        restricted_sessions,
        server.log()
    );
    assert!(
        !sessions_on(&admin_socket)
            .iter()
            .any(|s| s.starts_with("bob") || restricted_sessions.contains(s)),
        "{:?}",
        sessions_on(&admin_socket)
    );
    let keys =
        String::from_utf8_lossy(&tmux(&restricted_socket, &["list-keys"]).stdout).into_owned();
    for forbidden in [
        "command-prompt",
        "new-window",
        "split-window",
        "run-shell",
        "respawn",
        "choose-",
        "switch-client",
        "copy-pipe",
        "display-menu",
        "paste-buffer",
        "detach",
        "-T prefix",
    ] {
        assert!(!keys.contains(forbidden), "{forbidden} is bound:\n{keys}");
    }
    let prefix = String::from_utf8_lossy(
        &tmux(&restricted_socket, &["show-options", "-g", "prefix"]).stdout,
    )
    .into_owned();
    assert!(prefix.contains("None"), "{prefix}");

    let attempts: Vec<(&str, String)> = vec![
        ("word", format!("touch {}", home.mark_str("word"))),
        ("bang", format!("!touch {}", home.mark_str("bang"))),
        ("sh", format!("sh touch {}", home.mark_str("sh"))),
        ("os", format!("os touch {}", home.mark_str("os"))),
        (
            "chain",
            format!("show version + touch {}", home.mark_str("chain")),
        ),
        (
            "escaped-chain",
            format!("show version ++ touch {}", home.mark_str("escaped-chain")),
        ),
        (
            "pipeline-flags",
            format!("touch {} : --loop", home.mark_str("pipeline-flags")),
        ),
        (
            "serialized",
            format!(
                "chain -P '{}'",
                json!({"sub_args_line": format!("touch {}", home.mark_str("serialized"))})
            ),
        ),
        (
            "start-job",
            format!(
                "start job touch {} --name tpr-j",
                home.mark_str("start-job")
            ),
        ),
        (
            "daemon",
            format!("touch {} -d --name tpr-d", home.mark_str("daemon")),
        ),
        (
            "schedule",
            format!("sh touch {} -s 'every 1 second'", home.mark_str("schedule")),
        ),
        (
            "shell-flag",
            format!(
                "show version --shell && touch {}",
                home.mark_str("shell-flag")
            ),
        ),
        ("python", "python".to_string()),
        (
            "python-line",
            format!(
                "import pathlib; pathlib.Path('{}').touch()",
                home.mark_str("python-line")
            ),
        ),
        ("instance", "instance sql:other".to_string()),
        (
            "stack",
            format!("stack run touch {}", home.mark_str("stack")),
        ),
        (
            "compose",
            format!("compose run touch {}", home.mark_str("compose")),
        ),
        ("upgrade", "upgrade mrsm --yes".to_string()),
        ("install", "install plugin tpr-none --yes".to_string()),
        ("sql", "sql local".to_string()),
        ("edit-config", "edit config".to_string()),
        ("show-graph", "show graph -c sql:local".to_string()),
        (
            "copy-pipes",
            "copy pipes -c sql:local -m tpr_edit -i sql:local".to_string(),
        ),
        ("show-users", "show users".to_string()),
    ];
    for (name, line) in &attempts {
        let before = bob.out.len();
        bob.line(line).await;
        // Submit the next line once the denial has been followed by the new prompt.
        let answered = bob
            .wait_for(
                |out| prompt_follows(&out[before..], refused),
                Duration::from_secs(60),
            )
            .await;
        assert!(
            answered,
            "{name}: no refusal for {line:?}:\n{}\nServer log:\n{}",
            &bob.out[before..],
            server.log()
        );
        // Give tmux a different screen before the next refusal. Consecutive commands can
        // draw the same denial, which its incremental redraw may leave untouched.
        bob.ready().await;
    }
    // Require the actual listing before checking that it excluded the private job.
    let before = bob.out.len();
    bob.line("show jobs").await;
    assert!(
        bob.wait_for(
            |out| prompt_follows(&out[before..], |response| {
                response.contains("tpr-stdin")
                    || response.contains("No running or stopped jobs.")
                    || response.contains("Some jobs on this server are not yours to see")
            }),
            Duration::from_secs(60)
        )
        .await,
        "show jobs: {}",
        &bob.out[before..]
    );
    assert!(
        !bob.out[before..].contains("execute-tpr-hidden"),
        "{}",
        &bob.out[before..]
    );

    // Mutations, including a new job with a colliding name, cannot reach the hidden job.
    for line in [
        "attach logs execute-tpr-hidden",
        "delete jobs execute-tpr-hidden -y",
        "stop jobs execute-tpr-hidden -y",
        "show version -d --name execute-tpr-hidden",
    ] {
        let before = bob.out.len();
        bob.line(line).await;
        assert!(
            bob.wait_for(
                |out| prompt_follows(&out[before..], |response| {
                    response.contains("does not exist")
                        || response.contains("not yours to see")
                        || response.contains("Missing required scope")
                }),
                Duration::from_secs(60)
            )
            .await,
            "{line}: {}",
            &bob.out[before..]
        );
        bob.ready().await;
    }
    assert!(hidden_job.exists(), "bob deleted the administrator's job");
    let properties: Value = serde_json::from_slice(
        &std::fs::read(hidden_job.join("properties.json")).expect("administrator's job persists"),
    )
    .unwrap();
    assert_eq!(properties["owner"], "root", "job ownership changed");

    // The editor of an action the routes allow (`edit pipes`) never starts, and the terminal
    // says what to do instead.
    let before = bob.out.len();
    bob.line("edit pipes -c sql:local -m tpr_edit -i sql:local --yes")
        .await;
    assert!(
        bob.wait_for(
            |out| prompt_follows(&out[before..], |response| {
                response.contains("does not open an editor") && response.contains("--params")
            }),
            Duration::from_secs(60)
        )
        .await,
        "edit pipes: {}",
        &bob.out[before..]
    );
    // `attach job` shows a job's output but never forwards input to a job bob could not start.
    let before = bob.out.len();
    bob.line("attach job tpr-stdin").await;
    assert!(
        bob.wait_for(
            |out| out[before.min(out.len())..].contains("without forwarding input"),
            Duration::from_secs(60)
        )
        .await,
        "attach: {}",
        &bob.out[before..]
    );
    bob.line("yes").await;
    bob.drain(Duration::from_secs(2)).await;
    bob.send("\x03").await;
    bob.drain(Duration::from_secs(1)).await;

    // tmux: the prefix key and the command prompt reach nothing (the keys go to the shell).
    for (name, keys) in [
        ("tmux-new-window-key", "\x02c"),
        ("tmux-split-key", "\x02\""),
        ("tmux-choose-tree-key", "\x02s"),
    ] {
        bob.send(keys).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        bob.line(&format!("touch {}", home.mark_str(name))).await;
    }
    for (name, command) in [
        ("tmux-run-shell", "run-shell"),
        ("tmux-new-window", "new-window"),
        ("tmux-respawn", "respawn-pane -k"),
    ] {
        bob.send("\x02:").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        bob.line(&format!("{command} \"touch {}\"", home.mark_str(name)))
            .await;
    }
    // The console's new-window message opens a restricted shell too.
    bob.send_json(json!(["new_window"])).await;
    bob.drain(Duration::from_secs(2)).await;
    bob.ready().await;
    bob.line(&format!("touch {}", home.mark_str("new-window-message")))
        .await;
    bob.drain(Duration::from_secs(3)).await;
    // Only bob's session is on the restricted server, with windows of the restricted shell.
    let panes = String::from_utf8_lossy(
        &tmux(
            &restricted_socket,
            &[
                "list-panes",
                "-a",
                "-F",
                "#{session_name} #{pane_current_command}",
            ],
        )
        .stdout,
    )
    .into_owned();
    assert!(
        !panes.is_empty()
            && panes.lines().all(|line| {
                line.split_once(' ').is_some_and(|(name, command)| {
                    name == restricted_sessions[0] && !matches!(command, "sh" | "bash")
                })
            }),
        "{panes}"
    );

    let mut reached = Vec::new();
    for name in attempts.iter().map(|(n, _)| *n).chain([
        "editor",
        "job-input",
        "tmux-new-window-key",
        "tmux-split-key",
        "tmux-choose-tree-key",
        "tmux-run-shell",
        "tmux-new-window",
        "tmux-respawn",
        "new-window-message",
    ]) {
        if home.mark(name).exists() {
            reached.push(name);
        }
    }
    assert!(
        reached.is_empty(),
        "the non-administrator ran system commands: {reached:?}\n{}",
        bob.out
    );
    // The job is still waiting for its input.
    assert!(!home.mark("job-input").exists());
    drop(bob);

    // ---- The administrator: unaffected. ----
    let root_session = instance.session("root").await;
    let mut root = Term::open(port, &root_session).await;
    root.ready().await;
    root.line(&format!("touch {}", home.mark_str("admin-word")))
        .await;
    assert!(
        appears(&home.mark("admin-word"), Duration::from_secs(30)),
        "{}",
        root.out
    );
    root.line(&format!("!touch {}", home.mark_str("admin-bang")))
        .await;
    assert!(
        appears(&home.mark("admin-bang"), Duration::from_secs(30)),
        "{}",
        root.out
    );
    root.line("edit pipes -c sql:local -m tpr_edit -i sql:local --yes")
        .await;
    assert!(
        appears(&home.mark("editor"), Duration::from_secs(30)),
        "{}",
        root.out
    );
    // tmux's command prompt is the administrator's.
    root.drain(Duration::from_secs(2)).await;
    root.send("\x02:").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    root.line(&format!(
        "run-shell \"touch {}\"",
        home.mark_str("admin-run-shell")
    ))
    .await;
    assert!(
        appears(&home.mark("admin-run-shell"), Duration::from_secs(30)),
        "{}",
        root.out
    );
    drop(root);
    drop(server);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_administrator_loses_the_unrestricted_terminal_at_once() {
    if !tmux_available() {
        eprintln!("SKIP a_demoted_administrator_loses_the_unrestricted_terminal_at_once: no tmux");
        return;
    }
    let home = Home::new();
    home.register_user("root", true, &[]);
    home.register_user("dave", true, BOB_SCOPES);
    let server = serve(&home);
    let port = server.port;
    let instance = Instance::open(&home).await;
    let config = home.server_config();
    let admin_socket = config.webterm_resources_path.join("tmux.sock");
    let restricted_socket = config.webterm_resources_path.join("tmux-r.sock");
    instance.set_user_type("dave", "admin").await;

    let session = instance.session("dave").await;
    let mut dave = Term::open(port, &session).await;
    dave.ready().await;
    dave.line(&format!("touch {}", home.mark_str("before")))
        .await;
    assert!(
        appears(&home.mark("before"), Duration::from_secs(30)),
        "{}",
        dave.out
    );
    // A command left running in the administrator's session.
    dave.send("\x02c").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    dave.line(&format!(
        "sleep 25; touch {}",
        home.mark_str("left-running")
    ))
    .await;
    assert!(
        sessions_on(&admin_socket)
            .iter()
            .any(|s| s.starts_with("dave"))
    );

    // Demoted while the terminal is open: it closes at its next check, and the
    // administrator's session (with what runs in it) ends.
    instance.set_user_type("dave", "user").await;
    assert!(
        dave.wait_for(
            |out| out.contains("Your role changed"),
            Duration::from_secs(40)
        )
        .await,
        "{}",
        dave.out
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while sessions_on(&admin_socket)
        .iter()
        .any(|s| s.starts_with("dave"))
    {
        assert!(
            Instant::now() < deadline,
            "{:?}",
            sessions_on(&admin_socket)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(dave);

    // The next terminal is restricted.
    let mut dave = Term::open(port, &session).await;
    dave.ready().await;
    let before = dave.out.len();
    dave.line(&format!("touch {}", home.mark_str("after")))
        .await;
    assert!(
        dave.wait_for(
            |out| refused(&out[before.min(out.len())..]),
            Duration::from_secs(60)
        )
        .await,
        "{}",
        dave.out
    );
    assert!(
        sessions_on(&restricted_socket)
            .iter()
            .any(|s| s.starts_with("host-api-terminal-"))
    );
    tokio::time::sleep(Duration::from_secs(25)).await;
    assert!(!home.mark("after").exists());
    assert!(
        !home.mark("left-running").exists(),
        "the old session kept running"
    );

    // Promoted again: the restricted terminal closes, and the next one is unrestricted.
    instance.set_user_type("dave", "admin").await;
    assert!(
        dave.wait_for(
            |out| out.contains("Your role changed"),
            Duration::from_secs(40)
        )
        .await,
        "{}",
        dave.out
    );
    drop(dave);
    let mut dave = Term::open(port, &session).await;
    dave.ready().await;
    dave.line(&format!("touch {}", home.mark_str("promoted")))
        .await;
    assert!(
        appears(&home.mark("promoted"), Duration::from_secs(30)),
        "{}",
        dave.out
    );
    assert!(
        !sessions_on(&restricted_socket)
            .iter()
            .any(|s| s.starts_with("dave"))
    );
    drop(dave);
    drop(server);
}

/// A job that waits on `read` (an administrator's script that prints a line, then reads one),
/// answered as documented: `attach job` in the terminal, and the job log websocket
/// (`MRSM_STDIN`, then the line). Each case leaves a marker when the job gets the line; writing
/// to the job's stdin FIFO directly is the control (it does).
///
/// The supervisor now marks FIFO reads, so authorization and input forwarding must work
/// together through both transports.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_administrator_answers_a_jobs_prompt_through_attach_and_the_log_websocket() {
    if !tmux_available() {
        eprintln!("SKIP: no tmux");
        return;
    }
    let home = Home::new();
    home.register_user("root", true, &[]);
    let script = |name: &str| {
        let path = home.marks.path().join(format!("{name}.sh"));
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\necho waiting for a line\nread -r line && touch '{}'\n",
                home.mark_str(name)
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    };
    let start = |job: &'static str, mark: &str| {
        let out = home.mrsm(&["sh", &script(mark), "-d", "--name", job]);
        assert!(out.status.success(), "{out:?}");
        JobGuard(&home, job)
    };
    let _a = start("tpr-read-attach", "answered-by-attach");
    let _b = start("tpr-read-socket", "answered-by-websocket");
    let _c = start("tpr-read-fifo", "answered-by-fifo");
    let server = serve(&home);
    let port = server.port;
    let instance = Instance::open(&home).await;
    std::thread::sleep(Duration::from_secs(2));

    // The control: a line written to the job's stdin FIFO reaches it.
    let fifo = find(home.root.path(), "tpr-read-fifo")
        .map(|dir| dir.join("input.stdin"))
        .filter(|p| p.exists())
        .expect("the job's stdin FIFO");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&fifo)
        .unwrap()
        .write_all(b"yes\n")
        .unwrap();
    let by_fifo = appears(&home.mark("answered-by-fifo"), Duration::from_secs(10));

    // The job log websocket: the first message is the credential; the server sends `MRSM_STDIN`
    // when the job waits for input, and forwards the next message.
    let form = format!("username=root&password={PASSWORD}");
    let (_, login) = http(
        port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    )
    .unwrap();
    let token = serde_json::from_str::<Value>(&login).unwrap()["access_token"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, blocking) = http(
        port,
        "GET",
        "/jobs/tpr-read-socket/is_blocking_on_stdin",
        &[("Authorization", &format!("Bearer {token}"))],
        b"",
    )
    .unwrap();
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/logs/tpr-read-socket/ws"))
            .await
            .unwrap();
    ws.send(Message::Text(token.clone().into())).await.unwrap();
    let mut asked = false;
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while let Ok(Some(Ok(message))) = tokio::time::timeout_at(deadline, ws.next()).await {
        if message == Message::Text("MRSM_STDIN".into()) {
            asked = true;
            ws.send(Message::Text("yes\n".into())).await.unwrap();
            break;
        }
        seen.push(format!("{message:?}"));
    }
    let by_socket = appears(&home.mark("answered-by-websocket"), Duration::from_secs(10));

    // `attach job` in the administrator's terminal.
    let session = instance.session("root").await;
    let mut root = Term::open(port, &session).await;
    root.ready().await;
    root.line("attach job tpr-read-attach").await;
    root.drain(Duration::from_secs(5)).await;
    root.line("yes").await;
    let by_attach = appears(&home.mark("answered-by-attach"), Duration::from_secs(15));
    root.send("\x03").await;
    root.drain(Duration::from_secs(1)).await;
    drop(root);
    drop(server);
    assert!(
        by_fifo,
        "the control failed: the FIFO's line did not reach the job"
    );
    assert!(
        by_socket && by_attach,
        "is_blocking_on_stdin: {blocking}; websocket asked for input: {asked} (saw {seen:?}); \
         websocket answer reached the job: {by_socket}; attach answer reached the job: \
         {by_attach}"
    );
}
