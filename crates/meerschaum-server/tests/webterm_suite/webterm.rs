//! The web terminal (§7.1): page authentication, the websocket handshake, the terminado wire
//! protocol against a real PTY, resize, `--no-webterm`, concurrent terminals, and tmux reattach.
//!
//! Terminals run `sh` or `mrsm` in scratch roots (never the user's shell or a real root).
//! The plain path always runs;
//! the tmux test skips itself when no `tmux` is installed, and says so on stderr.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::{MemoryInstance, SECRET, send};
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use meerschaum_core::Value as CoreValue;
use meerschaum_core::config::default_config;
use meerschaum_server::access::Access;
use meerschaum_server::backends::ActionsBackend;
use meerschaum_server::webterm::remote::{Event, Input, Terminal};
use meerschaum_server::webterm::{self, SessionLookup, ShellCommand, TmuxSettings, WebtermConfig};
use meerschaum_server::{AppState, ServerConfig, ServerFlags};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

mod access;
mod isolated_teams;
mod job_context;
mod revocation;
mod shells;
mod tmux;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const ADMIN_SESSION: &str = "11111111-1111-4111-8111-111111111111";
const ADMIN_TAB_TWO: &str = "11111111-1111-4111-8111-222222222222";
const OTHER_ADMIN_SESSION: &str = "22222222-2222-4222-8222-222222222222";
/// `bob`, an operator (the role with `webterm:use`).
const USER_SESSION: &str = "33333333-3333-4333-8333-333333333333";
/// A session of a user name the instance does not have: v4 only asked the session store, so a
/// session with a user name was enough; in v5 the user must exist.
const GHOST_SESSION: &str = "44444444-4444-4444-8444-444444444444";
/// `ed`, a user without permissions of their own: the default role, `editor`, which has no
/// terminal.
const EDITOR_SESSION: &str = "55555555-5555-4555-8555-555555555555";
const UNKNOWN_SESSION: &str = "88888888-8888-4888-8888-888888888888";

/// The console's session store: a test may end a session (a logout, or an administrator ending
/// the user's sessions) or make the store fail while a terminal is open.
struct Sessions {
    map: Mutex<HashMap<String, String>>,
    failing: AtomicBool,
}

impl Sessions {
    fn end(&self, session_id: &str) {
        self.map.lock().unwrap().remove(session_id);
    }

    fn fail(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }
}

#[async_trait]
impl SessionLookup for Sessions {
    async fn username(&self, session_id: &str) -> Result<Option<String>, String> {
        if self.failing.load(Ordering::SeqCst) {
            return Err("the session store is down".into());
        }
        Ok(self.map.lock().unwrap().get(session_id).cloned())
    }

    async fn binding(&self, session_id: &str) -> Result<Option<(String, String)>, String> {
        Ok(self
            .username(session_id)
            .await?
            .map(|user| (user, "test-binding".into())))
    }
}

struct HostTerminal;

#[async_trait]
impl ActionsBackend for HostTerminal {
    fn list(&self) -> Vec<String> {
        Vec::new()
    }

    async fn execute(&self, _: &str, _: meerschaum_core::Map) -> meerschaum_core::SuccessTuple {
        panic!("The console bridge must only use the configured terminal backend")
    }

    async fn host_api_terminal(
        &self,
        team: Option<uuid::Uuid>,
        caller: &Access,
    ) -> Result<Terminal, String> {
        assert!(team.is_none());
        assert_eq!(caller.username(), Some("bob"));
        assert_eq!(caller.credential.as_deref(), Some("test-binding"));
        let (input, mut commands) = tokio::sync::mpsc::channel(4);
        let (output, events) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let _ = output.send(Event::Setup).await;
            while let Some(command) = commands.recv().await {
                if matches!(command, Input::Stdin(text) if text == "fixture command\n") {
                    let _ = output
                        .send(Event::Output("fixture response\n".into()))
                        .await;
                }
            }
        });
        Ok(Terminal { input, events })
    }
}

fn sessions() -> Arc<Sessions> {
    Arc::new(Sessions {
        map: Mutex::new(
            [
                (ADMIN_SESSION, "admin"),
                (ADMIN_TAB_TWO, "admin"),
                (OTHER_ADMIN_SESSION, "root2"),
                (USER_SESSION, "bob"),
                (GHOST_SESSION, "nobody"),
                (EDITOR_SESSION, "ed"),
            ]
            .into_iter()
            .map(|(id, user)| (id.to_string(), user.to_string()))
            .collect(),
        ),
        failing: AtomicBool::new(false),
    })
}

/// The default server configuration (`default_config()`, as `start api` reads it), with the
/// `start api` flags and an optional override of `api:permissions:actions:non_admin`.
fn server_config(root: &Path, flags: ServerFlags, non_admin: Option<bool>) -> ServerConfig {
    let mut map = default_config(root.to_str().unwrap(), 4, false);
    if let Some(value) = non_admin {
        let Some(CoreValue::Map(api)) = map.get_mut("api") else {
            panic!("the default config has an `api` section");
        };
        let Some(CoreValue::Map(permissions)) = api.get_mut("permissions") else {
            panic!("`api:permissions`");
        };
        // v5 no longer ships `api:permissions:actions`: a config that sets it adds it.
        let actions = permissions
            .entry("actions".to_string())
            .or_insert_with(|| CoreValue::Map(Default::default()));
        let CoreValue::Map(actions) = actions else {
            panic!("`api:permissions:actions` is a map");
        };
        actions.insert("non_admin".into(), CoreValue::Bool(value));
    }
    let mut config = ServerConfig::from_map(
        &map,
        &ServerFlags {
            instance: Some("sql:fixture".into()),
            ..flags
        },
    );
    let paths = ServerConfig::for_root("sql:fixture", root);
    config.secret_key_path = paths.secret_key_path;
    config.plugins_archives_path = paths.plugins_archives_path;
    config.webterm_resources_path = paths.webterm_resources_path;
    config.root_dir = paths.root_dir;
    config
}

/// A state over the default configuration, with two administrators, an operator (`bob`) and a
/// user with the default role (`ed`).
fn state(root: &Path) -> AppState {
    state_from(server_config(root, ServerFlags::default(), None))
}

fn state_from(cfg: ServerConfig) -> AppState {
    state_and_instance(cfg).0
}

/// [`state_from`] and its instance, whose users a test may change.
fn state_and_instance(cfg: ServerConfig) -> (AppState, Arc<MemoryInstance>) {
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("root2", "pass", Some("admin"));
    instance.seed_user("bob", "pass", None);
    let operator: Vec<String> = meerschaum_core::access::Role::Operator
        .permissions()
        .into_iter()
        .collect();
    let operator: Vec<&str> = operator.iter().map(String::as_str).collect();
    instance.seed_user_scopes("bob", &operator);
    instance.seed_user("ed", "pass", None);
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .actions(Arc::new(HostTerminal))
        .build()
        .expect("state");
    (state, instance)
}

fn config(state: &AppState, shell: ShellCommand, tmux: Option<TmuxSettings>) -> WebtermConfig {
    config_with(state, sessions(), shell, tmux)
}

/// [`config`] over a session store the test keeps.
fn config_with(
    state: &AppState,
    sessions: Arc<Sessions>,
    shell: ShellCommand,
    tmux: Option<TmuxSettings>,
) -> WebtermConfig {
    WebtermConfig::new(
        state,
        sessions,
        Arc::new(|id| format!("<html><body data-session=\"{id}\">terminal page</body></html>")),
        shell,
    )
    .with_tmux(tmux)
}

fn sh() -> ShellCommand {
    // A bare `sh` on a PTY is interactive; `PS1` keeps the prompt short and predictable.
    let mut shell = ShellCommand::new("sh");
    shell.env.push(("PS1".into(), "$ ".into()));
    shell
}

/// Serve the terminal routes on a random loopback port.
async fn serve(router: Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

async fn connect(addr: std::net::SocketAddr, session: &str) -> Socket {
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/websocket/{session}"))
        .await
        .expect("handshake");
    socket
}

async fn send_json(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

/// The next terminado message, skipping pings; `Ok(None)` once the socket closes and `Err` when
/// nothing arrives for 20 seconds.
async fn try_next_message(socket: &mut Socket) -> Result<Option<Value>, ()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => return serde_json::from_str(text.as_str()).ok(),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return None,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .map_err(|_| ())
}

async fn next_message(socket: &mut Socket) -> Option<Value> {
    try_next_message(socket)
        .await
        .expect("timed out waiting for the terminal")
}

/// Read `stdout` frames until `done(accumulated)` holds; panics with the output on timeout.
async fn read_until(socket: &mut Socket, done: impl Fn(&str) -> bool) -> String {
    let mut out = String::new();
    // An overall deadline: a repainting terminal (tmux's status line) never goes quiet.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    while !done(&out) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the expected output; got {out:?}"
        );
        match try_next_message(socket)
            .await
            .unwrap_or_else(|_| panic!("the terminal went quiet; got {out:?}"))
        {
            Some(message) if message[0] == "stdout" => {
                out.push_str(message[1].as_str().unwrap());
            }
            Some(_) => {}
            None => panic!("the terminal closed before the expected output; got {out:?}"),
        }
    }
    out
}

async fn expect_setup(socket: &mut Socket) {
    let message = next_message(socket).await.expect("setup");
    assert_eq!(message, json!(["setup", {}]));
}

/// The text without terminal escape sequences (CSI and OSC), as the shell's readline emits them.
fn plain(out: &str) -> String {
    let mut text = String::new();
    let mut chars = out.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            text.push(c);
            continue;
        }
        match chars.next() {
            Some('[') => {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        // tmux repaints with cursor moves instead of newlines.
                        if matches!(c, 'H' | 'f' | 'd' | 'B' | 'E' | 'J') {
                            text.push('\n');
                        }
                        break;
                    }
                }
            }
            Some(']') => {
                // OSC ends with BEL or ST (ESC \\).
                while let Some(c) = chars.next() {
                    if c == '\u{7}' {
                        break;
                    }
                    if c == '\u{1b}' {
                        chars.next();
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    text
}

/// A line made only of `token` (the shell's answer, not the echo of the command that asked).
fn has_line(out: &str, token: &str) -> bool {
    plain(out).split(['\r', '\n']).any(|line| line == token)
}

/// A line with the prefix `start` ending in `end`.
fn has_line_matching(out: &str, start: &str, end: &str) -> bool {
    plain(out)
        .split(['\r', '\n'])
        .any(|l| l.starts_with(start) && l.ends_with(end))
}

/// How a terminal's socket ended: the `stdout` text before the end, whether
/// `["disconnect", 1]` came, and the close frame's code. Panics when it is still open after
/// `within`.
async fn ending(socket: &mut Socket, within: Duration) -> (String, bool, Option<u16>) {
    let mut out = String::new();
    let mut disconnected = false;
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, socket.next()).await {
            Err(_) => panic!("the terminal stayed open; got {:?}", plain(&out)),
            Ok(Some(Ok(Message::Text(text)))) => {
                let message: Value = serde_json::from_str(text.as_str()).unwrap();
                if message[0] == "stdout" {
                    out.push_str(message[1].as_str().unwrap());
                } else if message == json!(["disconnect", 1]) {
                    disconnected = true;
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                return (out, disconnected, frame.map(|f| u16::from(f.code)));
            }
            Ok(None) | Ok(Some(Err(_))) => return (out, disconnected, None),
            Ok(Some(Ok(_))) => {}
        }
    }
}

/// How often the revocation tests' terminals authorize their sessions again.
const RECHECK: Duration = Duration::from_millis(300);

/// The process id of the shell behind a terminal (`echo $$`).
async fn shell_pid(socket: &mut Socket) -> i32 {
    send_json(socket, json!(["stdin", "echo shellpid:$$:end\n"])).await;
    let out = read_until(socket, |out| has_line_matching(out, "shellpid:", ":end")).await;
    plain(&out)
        .split(['\r', '\n'])
        .find(|l| l.starts_with("shellpid:") && l.ends_with(":end"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|n| n.parse().ok())
        .expect("the shell's pid")
}

fn alive(pid: i32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// Wait for a shell to end (`/proc` is Linux-only; elsewhere this checks nothing).
async fn assert_shell_ends(pid: i32) {
    if !Path::new("/proc/self").exists() {
        return;
    }
    for _ in 0..100 {
        if !alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the shell {pid} outlived its revoked terminal");
}

/// A closed terminal takes no more input: a send fails, or nothing echoes it.
async fn assert_no_echo(socket: &mut Socket) {
    if socket
        .send(Message::Text(
            json!(["stdin", "echo after-revocation\n"])
                .to_string()
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    let echoed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) if text.as_str().contains("after-revocation") => {
                    return true;
                }
                Some(Ok(_)) => continue,
                _ => return false,
            }
        }
    })
    .await;
    assert!(
        !matches!(echoed, Ok(true)),
        "a revoked terminal echoed input"
    );
}

fn short_tmp() -> tempfile::TempDir {
    // tmux's socket path must fit in `sun_path` (about 100 bytes).
    let base = std::env::temp_dir();
    let base = if base.as_os_str().len() > 40 {
        "/tmp".into()
    } else {
        base
    };
    tempfile::Builder::new()
        .prefix("mt")
        .tempdir_in(base)
        .unwrap()
}

/// Every tmux command, including the terminal's launch and new-window requests, addresses
/// this private socket with `-S`.
fn tmux_socket(dir: &Path) -> std::path::PathBuf {
    dir.join("server.sock")
}

/// Kills the test's private tmux server (by its socket, never the default one) even when an
/// assertion fails.
struct PrivateTmux(std::path::PathBuf);

impl Drop for PrivateTmux {
    fn drop(&mut self) {
        let _ = std::process::Command::new("tmux")
            .arg("-S")
            .arg(tmux_socket(&self.0))
            .arg("kill-server")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output();
    }
}
