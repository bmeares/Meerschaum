//! The web terminal (`docs/design/v5.0.0.md` §7.1 "Webterm"): `GET /webterm/{session_id}` (the
//! standalone page) and the websocket `/websocket/{session_id}`, which bridges one browser tab to
//! one PTY (`portable-pty`).
//!
//! v4 started a Tornado/terminado process next to the API and proxied to it; v5 serves both
//! routes here and keeps v4's observable model:
//!
//! - **Authentication** is the console session store. The session id is the path segment
//!   (`is_session_authenticated(session_id)`); the user name behind it names the terminal.
//!   An unknown session gets v4's `401` "Unauthorized / Please log in" page (and a websocket
//!   that is accepted and closed straight away). The gate is v4's `is_session_authenticated`
//!   (`api/dash/sessions.py`): when `api:permissions:actions:non_admin` is true (its default;
//!   `start api --secure` sets it to false) any logged-in user gets a shell, otherwise only an
//!   administrator does. No scopes are consulted. This is **not** the gate of `POST /actions`
//!   (`is_user_allowed_to_execute`), which reads `system:api:permissions:actions:non_admin`, a key
//!   v4's default config does not define (so it refuses non-admins unless it is set).
//! - **tmux.** With `api:webterm:tmux:enabled` (default) and a `tmux` binary, the shell runs in
//!   `tmux new-session -A -s <user><session_suffix>`, so a reconnect (or a second tab) attaches
//!   to the same session. Closing the socket kills the PTY and with it the tmux client, never the
//!   tmux session. Without tmux each connection runs its own shell.
//! - **The shell** is `<this binary> -i <instance>` (the `mrsm` shell on the server's instance)
//!   started with the server's environment minus `COLUMNS`/`LINES` (and `TMUX`).
//!   On the tmux path the environment travels in a `0600` file in the webterm resources
//!   directory that the first shell sources (a pre-existing tmux server would otherwise hand
//!   the shell its own, older, environment, and argv would expose secrets to `ps`).
//! - **Wire protocol** is terminado's: the client sends JSON arrays `["stdin", text]` and
//!   `["set_size", rows, cols, height, width]`; the server sends `["setup", {}]`,
//!   `["stdout", text]` and `["disconnect", 1]`. `["new_window"]` is a v5 addition (tmux only):
//!   it opens a new tmux window running the shell, which v4 did by typing into the terminal.
//!
//! The crate does not know the console's session store (`meerschaum-web` depends on this crate),
//! so [`WebtermConfig`] carries a [`SessionLookup`] and the page renderer; `meerschaum-web`
//! builds both and merges [`routes`] into the console router.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as UrlPath, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use portable_pty::{ChildKiller, CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::AppState;

/// The most terminals one server runs at once (every tab is a process).
pub const MAX_TERMINALS: usize = 128;

/// The largest websocket message accepted from a browser (a paste).
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// Messages waiting for the PTY's writer thread per terminal. A shell that stops reading (or a
/// client that floods it) fills this, and the connection is closed instead of buffering forever.
const INPUT_QUEUE: usize = 64;

/// `["new_window"]` requests one connection may make (each is a process on the tmux path).
const MAX_NEW_WINDOWS: usize = 8;

/// The websocket close code for "going away / try again later" when [`MAX_TERMINALS`] is reached.
const CLOSE_TRY_AGAIN: u16 = 1013;

static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// The console's session store, as far as the terminal needs it.
#[async_trait]
pub trait SessionLookup: Send + Sync {
    /// The user name logged in under `session_id`; `Ok(None)` for an unknown or expired session.
    async fn username(&self, session_id: &str) -> Result<Option<String>, String>;
}

/// Renders the standalone terminal page for a session id.
pub type PageRenderer = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// The program a terminal runs (without tmux's wrapping).
#[derive(Debug, Clone)]
pub struct ShellCommand {
    /// The executable (this binary for the `mrsm` shell).
    pub program: String,
    /// Its arguments.
    pub args: Vec<String>,
    /// Extra environment variables for the PTY's process (and the tmux client, on the tmux path).
    pub env: Vec<(String, String)>,
    /// The working directory (default: the server's).
    pub cwd: Option<PathBuf>,
}

impl ShellCommand {
    /// A command with no arguments.
    pub fn new(program: impl Into<String>) -> ShellCommand {
        ShellCommand {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            cwd: None,
        }
    }

    /// This program's shell on `instance_keys` (`<self> -i <instance>`, where `<self>` is the
    /// `mrsm` binary or `python -m meerschaum`), as v4 started `get_shell(mrsm_instance=...)`.
    pub fn current_exe_shell(instance_keys: &str) -> std::io::Result<ShellCommand> {
        let (program, mut args) = meerschaum_core::self_exe::self_program();
        let mut shell = ShellCommand::new(program);
        args.extend(["-i".into(), instance_keys.into()]);
        shell.args = args;
        Ok(shell)
    }
}

/// How the tmux path names, creates and wraps sessions.
#[derive(Debug, Clone)]
pub struct TmuxSettings {
    /// The tmux executable.
    pub binary: String,
    /// Appended to the user name (`api:webterm:tmux:session_suffix`).
    pub session_suffix: String,
    /// The directory of the shell's environment file (created `0700`).
    pub env_dir: PathBuf,
    /// `tmux -f <file>`: the configuration the tmux server starts with (`None`: tmux's own
    /// default, the user's `~/.tmux.conf`). Tests pass `/dev/null` so a developer's plugins and
    /// status line never run.
    pub config_file: Option<String>,
}

/// Everything the terminal routes need besides the server state.
#[derive(Clone)]
pub struct WebtermConfig {
    /// The console session store.
    pub sessions: Arc<dyn SessionLookup>,
    /// The standalone page.
    pub page: PageRenderer,
    /// The shell to run.
    pub shell: ShellCommand,
    /// `Some` runs the shell inside a tmux session.
    pub tmux: Option<TmuxSettings>,
}

impl WebtermConfig {
    /// A configuration from the server's `api:webterm:tmux:*` settings: tmux is used when it is
    /// enabled and installed.
    pub fn new(
        state: &AppState,
        sessions: Arc<dyn SessionLookup>,
        page: PageRenderer,
        mut shell: ShellCommand,
    ) -> WebtermConfig {
        let state_dir = state.config.webterm_resources_path.join("shell-state");
        if create_private_dir(&state_dir).is_ok() {
            shell.env.push((
                "MRSM_WEBTERM_STATE_DIR".into(),
                state_dir.to_string_lossy().into_owned(),
            ));
        }
        let tmux =
            (state.config.webterm_tmux_enabled && tmux_available("tmux", &shell.env)).then(|| {
                TmuxSettings {
                    binary: "tmux".into(),
                    session_suffix: state.config.webterm_tmux_session_suffix.clone(),
                    env_dir: state.config.webterm_resources_path.clone(),
                    config_file: None,
                }
            });
        WebtermConfig {
            sessions,
            page,
            shell,
            tmux,
        }
    }

    /// Replace the tmux settings (`None` forces the plain path).
    pub fn with_tmux(mut self, tmux: Option<TmuxSettings>) -> WebtermConfig {
        self.tmux = tmux;
        self
    }

    /// Whether this configuration keeps terminals in tmux sessions.
    pub fn uses_tmux(&self) -> bool {
        self.tmux.is_some()
    }
}

/// Whether `binary` runs (`tmux -V`).
pub fn tmux_available(binary: &str, env: &[(String, String)]) -> bool {
    std::process::Command::new(binary)
        .arg("-V")
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[derive(Clone)]
struct Inner {
    state: AppState,
    config: WebtermConfig,
}

/// The `/webterm/{session_id}` and `/websocket/{session_id}` routes. Merge them only when the
/// terminal is served (`!no_webterm`, the console is on); without them both paths answer `404`.
pub fn routes(state: AppState, config: WebtermConfig) -> Router {
    Router::new()
        .route("/webterm/{session_id}", get(page))
        .route("/webterm/{session_id}/", get(page))
        .route("/websocket/{session_id}", get(socket))
        .route("/websocket/{session_id}/", get(socket))
        .with_state(Inner { state, config })
}

/// The user a session may open a terminal as, or why not.
enum Access {
    Granted(String),
    Denied,
    Unavailable,
}

async fn authorize(inner: &Inner, session_id: &str) -> Access {
    // PARITY: `is_session_authenticated` is true for everyone under `--no-auth`; the terminal is
    // named after the session then (`username or session_id`).
    if inner.state.config.no_auth {
        return Access::Granted(session_id.to_string());
    }
    let username = match inner.config.sessions.username(session_id).await {
        Ok(Some(username)) if !username.is_empty() => username,
        Ok(_) => return Access::Denied,
        Err(error) => {
            tracing::warn!("The session store failed for a terminal: {error}");
            return Access::Unavailable;
        }
    };
    // PARITY: v4's `is_session_authenticated`: `api:permissions:actions:non_admin` (default true,
    // false under `--secure`) lets every logged-in user in, else only administrators
    // (`session_is_admin`).
    if !inner.state.config.actions_non_admin {
        let user = meerschaum_core::users::User::new(&username, None);
        let user_type = inner
            .state
            .instance()
            .get_user_type(&user)
            .await
            .ok()
            .flatten();
        if user_type.as_deref() != Some("admin") {
            return Access::Denied;
        }
    }
    Access::Granted(username)
}

const UNAUTHORIZED_PAGE: &str = r#"<html>
    <head>
        <style>
        body { background-color: #222222; }
        #message-div { padding: 15px; font-size: x-large; font-family: sans-serif; }
        h2 { color: #FFFFFF; }
        p { color: #FFFFFF; }
        a { color: #FFFFFF; text-decoration: underline; }
        </style>
    </head>
    <body>
        <div id="message-div">
            <h2>Unauthorized</h2>
            <p>Please <a href="/dash/login">log in</a> and try again.</p>
        </div>
    </body>
</html>
"#;

async fn page(State(inner): State<Inner>, UrlPath(session_id): UrlPath<String>) -> Response {
    let mut response = match authorize(&inner, &session_id).await {
        Access::Granted(_) => Html((inner.config.page)(&session_id)).into_response(),
        Access::Denied => (StatusCode::UNAUTHORIZED, Html(UNAUTHORIZED_PAGE)).into_response(),
        Access::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "The session store is unavailable.",
        )
            .into_response(),
    };
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, "no-store".parse().expect("header"));
    headers.insert("x-content-type-options", "nosniff".parse().expect("header"));
    // The session id is in this URL: never leak it as a referrer.
    headers.insert("referrer-policy", "no-referrer".parse().expect("header"));
    response
}

/// Whether a websocket's `Origin` may open a terminal: absent (not a browser), or the same
/// authority as the page that was asked for (`Host`, or `X-Forwarded-Host` behind a proxy).
/// Browsers always send `Origin` on a websocket handshake, and the session id travels in the URL,
/// not a cookie, so this is what stops another site from driving a terminal (cross-site
/// websocket hijacking), notably under `--no-auth`, where every session id is accepted.
pub fn origin_allowed(headers: &axum::http::HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Some(authority) = origin
        .to_str()
        .ok()
        .and_then(|o| o.split_once("://"))
        .map(|(_, rest)| rest.split('/').next().unwrap_or(""))
    else {
        return false;
    };
    let hosts = [header::HOST.as_str(), "x-forwarded-host"]
        .into_iter()
        .filter_map(|name| headers.get(name)?.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim);
    hosts
        .into_iter()
        .any(|host| !host.is_empty() && host.eq_ignore_ascii_case(authority))
}

async fn socket(
    State(inner): State<Inner>,
    UrlPath(session_id): UrlPath<String>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !origin_allowed(&headers) {
        return (StatusCode::FORBIDDEN, "Cross-origin websocket refused.").into_response();
    }
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            // PARITY: v4 accepts the websocket, then closes it when the session is not authorized.
            match authorize(&inner, &session_id).await {
                Access::Granted(name) => run(inner.config, name, socket).await,
                Access::Denied | Access::Unavailable => close(socket, 1000).await,
            }
        })
}

async fn close(mut socket: WebSocket, code: u16) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: "".into(),
        })))
        .await;
}

fn text(parts: Value) -> Message {
    Message::Text(parts.to_string().into())
}

/// Bridge one websocket to one PTY until either side ends.
async fn run(config: WebtermConfig, term_name: String, mut socket: WebSocket) {
    let guard = match TerminalGuard::acquire() {
        Some(guard) => guard,
        None => {
            close(socket, CLOSE_TRY_AGAIN).await;
            return;
        }
    };
    let launch = {
        let config = config.clone();
        let name = term_name.clone();
        tokio::task::spawn_blocking(move || launch(&config, &name)).await
    };
    let (pty, tmux_session) = match launch {
        Ok(Ok(started)) => started,
        Ok(Err(error)) => {
            tracing::warn!("Failed to start a terminal for '{term_name}': {error}");
            let _ = socket
                .send(text(json!([
                    "stdout",
                    format!("Failed to start the shell: {error}\r\n")
                ])))
                .await;
            let _ = socket.send(text(json!(["disconnect", 1]))).await;
            close(socket, 1011).await;
            return;
        }
        Err(_) => return,
    };
    let _guard = guard;
    let Pty {
        process_id,
        master,
        input,
        mut output,
        killer,
    } = pty;
    let (mut tx, mut rx) = socket.split();
    if tx.send(text(json!(["setup", {}]))).await.is_err() {
        return;
    }
    let mut decoder = Utf8Stream::default();
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await;
    let mut closed_by_client = false;
    let mut new_windows = 0usize;
    loop {
        tokio::select! {
            chunk = output.recv() => match chunk {
                Some(bytes) => {
                    let out = decoder.push(&bytes);
                    if !out.is_empty() && tx.send(text(json!(["stdout", out]))).await.is_err() {
                        closed_by_client = true;
                        break;
                    }
                }
                None => {
                    let rest = decoder.finish();
                    if !rest.is_empty() {
                        let _ = tx.send(text(json!(["stdout", rest]))).await;
                    }
                    let _ = tx.send(text(json!(["disconnect", 1]))).await;
                    break;
                }
            },
            message = rx.next() => match message {
                Some(Ok(Message::Text(raw))) => {
                    match parse_client_message(raw.as_str()) {
                        Some(ClientMessage::Stdin(data)) => {
                            // Never block the bridge on a stuck shell (its output could not be
                            // drained meanwhile): a full queue ends the connection.
                            if let Err(std::sync::mpsc::TrySendError::Full(_)) =
                                input.try_send(data.into_bytes())
                            {
                                tracing::warn!("A terminal's input queue is full; closing it.");
                                let _ = tx
                                    .send(text(json!(["stdout", "\r\n[input queue full]\r\n"])))
                                    .await;
                                let _ = tx.send(text(json!(["disconnect", 1]))).await;
                                break;
                            }
                        }
                        Some(ClientMessage::SetSize(size)) => {
                            let _ = master.resize(size);
                        }
                        Some(ClientMessage::NewWindow) => {
                            // Each window is a process: bounded per connection, and refused
                            // while the server is at its terminal limit.
                            if new_windows < MAX_NEW_WINDOWS
                                && active_terminals() < MAX_TERMINALS
                                && let (Some(settings), Some(session)) =
                                    (&config.tmux, &tmux_session)
                            {
                                new_windows += 1;
                                let (settings, shell, session) =
                                    (settings.clone(), config.shell.clone(), session.clone());
                                let _ = tokio::task::spawn_blocking(move || {
                                    tmux_new_window(&settings, &shell, &session)
                                })
                                .await;
                            }
                        }
                        Some(ClientMessage::ShellState(request)) => {
                            let (config, session) = (config.clone(), tmux_session.clone());
                            let snapshot = tokio::task::spawn_blocking(move || {
                                shell_state(&config, session.as_deref(), process_id)
                            }).await.ok().flatten();
                            if tx.send(text(json!(["shell_state", request, snapshot]))).await.is_err() {
                                break;
                            }
                        }
                        None => {}
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                    closed_by_client = true;
                    break;
                }
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => {
                if tx.send(Message::Ping(Vec::new().into())).await.is_err() {
                    closed_by_client = true;
                    break;
                }
            }
        }
    }
    if !closed_by_client {
        let _ = tx
            .send(Message::Close(Some(CloseFrame {
                code: 1000,
                reason: "".into(),
            })))
            .await;
    }
    // End the process on the PTY (the tmux client, never the tmux session) first, then close the
    // input queue: the writer thread drops its writer only after the process has exited, so the
    // EOF it sends reaches no one (see `spawn_pty`).
    drop(killer);
    drop(input);
}

/// What a browser may send.
enum ClientMessage {
    Stdin(String),
    SetSize(PtySize),
    NewWindow,
    ShellState(String),
}

/// Decode terminado's JSON arrays; anything else is ignored, as terminado does.
fn parse_client_message(raw: &str) -> Option<ClientMessage> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let items = value.as_array()?;
    match items.first()?.as_str()? {
        "stdin" => Some(ClientMessage::Stdin(items.get(1)?.as_str()?.to_string())),
        "set_size" => {
            let dim = |i: usize| items.get(i).and_then(Value::as_u64);
            let (rows, cols) = (dim(1)?, dim(2)?);
            if rows == 0 || cols == 0 {
                return None;
            }
            let clamp = |n: u64| u16::try_from(n).unwrap_or(u16::MAX);
            Some(ClientMessage::SetSize(PtySize {
                rows: clamp(rows),
                cols: clamp(cols),
                pixel_height: clamp(dim(3).unwrap_or(0)),
                pixel_width: clamp(dim(4).unwrap_or(0)),
            }))
        }
        "new_window" => Some(ClientMessage::NewWindow),
        "shell_state" => {
            let request = items.get(1)?.as_str()?;
            (request.len() <= 128).then(|| ClientMessage::ShellState(request.to_string()))
        }
        _ => None,
    }
}

/// Counts running terminals against [`MAX_TERMINALS`].
struct TerminalGuard;

impl TerminalGuard {
    fn acquire() -> Option<TerminalGuard> {
        if ACTIVE.fetch_add(1, Ordering::SeqCst) >= MAX_TERMINALS {
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How many terminals are running right now.
pub fn active_terminals() -> usize {
    ACTIVE.load(Ordering::SeqCst)
}

/// Incremental UTF-8 decoding of PTY output (a multi-byte character may straddle two reads);
/// invalid bytes become U+FFFD, as terminado's `replace` decoder does.
#[derive(Default)]
struct Utf8Stream {
    pending: Vec<u8>,
}

impl Utf8Stream {
    fn push(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut out = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(valid) => {
                    out.push_str(valid);
                    self.pending.clear();
                    return out;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    out.push_str(&String::from_utf8_lossy(&self.pending[..valid]));
                    match error.error_len() {
                        Some(bad) => {
                            out.push('\u{FFFD}');
                            self.pending.drain(..valid + bad);
                        }
                        None => {
                            self.pending.drain(..valid);
                            return out;
                        }
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> String {
        let rest = String::from_utf8_lossy(&self.pending).into_owned();
        self.pending.clear();
        rest
    }
}

/// A running PTY: output arrives on `output`, input goes through `input`.
struct Pty {
    process_id: Option<u32>,
    master: Box<dyn MasterPty + Send>,
    input: std::sync::mpsc::SyncSender<Vec<u8>>,
    output: mpsc::Receiver<Vec<u8>>,
    killer: KillOnDrop,
}

struct KillOnDrop(Box<dyn ChildKiller + Send + Sync>);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn pty_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

fn spawn_pty(builder: CommandBuilder) -> std::io::Result<Pty> {
    let pair = native_pty_system()
        .openpty(PtySize::default())
        .map_err(pty_error)?;
    let mut child = pair.slave.spawn_command(builder).map_err(pty_error)?;
    let process_id = child.process_id();
    // The child holds the slave end now; ours must close for EOF to reach the reader.
    drop(pair.slave);
    let killer = KillOnDrop(child.clone_killer());
    let mut reader = pair.master.try_clone_reader().map_err(pty_error)?;
    let mut writer = pair.master.take_writer().map_err(pty_error)?;
    // Reap the child whenever it ends, and tell the writer thread (see below).
    let (exited_tx, exited_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let _ = child.wait();
        let _ = exited_tx.send(());
    });
    let (out_tx, output) = mpsc::channel::<Vec<u8>>(64);
    std::thread::spawn(move || {
        let mut buffer = [0u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if out_tx.blocking_send(buffer[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let (input, input_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(INPUT_QUEUE);
    std::thread::spawn(move || {
        while let Ok(bytes) = input_rx.recv() {
            if writer.write_all(&bytes).is_err() || writer.flush().is_err() {
                break;
            }
        }
        // portable-pty's writer sends a newline and Ctrl-D (EOF) into the PTY when it is dropped.
        // While the tmux client is still alive it forwards those keys to the pane, and the
        // session's shell exits: the session would die with the tab. So the writer is dropped
        // only once the process on the PTY is gone (the bridge kills it before closing `input`).
        let _ = exited_rx.recv_timeout(Duration::from_secs(5));
    });
    Ok(Pty {
        process_id,
        master: pair.master,
        input,
        output,
        killer,
    })
}

/// Read the selected shell's buffer status; tmux itself never forwards OSC 133 reliably.
/// The target is this authenticated terminal's own session, never a process-table scan.
fn shell_state(
    config: &WebtermConfig,
    session: Option<&str>,
    process_id: Option<u32>,
) -> Option<Value> {
    let dir = config
        .shell
        .env
        .iter()
        .find(|(key, _)| key == "MRSM_WEBTERM_STATE_DIR")?
        .1
        .clone();
    let pid = if let (Some(tmux), Some(session)) = (&config.tmux, session) {
        let output = std::process::Command::new(&tmux.binary)
            .args(["display-message", "-p", "-t", session, "#{pane_pid}"])
            .envs(config.shell.env.iter().map(|(k, v)| (k, v)))
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        String::from_utf8(output.stdout)
            .ok()?
            .trim()
            .parse::<u32>()
            .ok()?
    } else {
        process_id?
    };
    read_shell_state(Path::new(&dir), pid)
}

fn read_shell_state(dir: &Path, pid: u32) -> Option<Value> {
    let path = dir.join(format!("{pid}.json"));
    let snapshot: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    if snapshot.get("pid")?.as_u64()? != u64::from(pid) {
        return None;
    }
    // A crashed shell's old file must never authorize a reused PID.
    #[cfg(target_os = "linux")]
    {
        let process = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let start = process.rsplit_once(") ")?.1.split_whitespace().nth(19)?;
        if snapshot.get("start")?.as_str()? != start {
            return None;
        }
    }
    Some(
        json!({"idle": snapshot.get("idle")?.as_bool()?, "empty": snapshot.get("empty")?.as_bool()?}),
    )
}

/// The tmux session name for a terminal: tmux rejects `.` and `:` in names.
pub fn tmux_session_name(term_name: &str, suffix: &str) -> String {
    format!("{term_name}{suffix}").replace(['.', ':'], "_")
}

/// Start a terminal's process: the shell itself, or `tmux new-session -A` around it. The second
/// value is the tmux session name on the tmux path.
fn launch(config: &WebtermConfig, term_name: &str) -> std::io::Result<(Pty, Option<String>)> {
    let cwd = match &config.shell.cwd {
        Some(dir) => Some(dir.clone()),
        None => std::env::current_dir().ok(),
    };
    let mut builder;
    let mut session = None;
    match &config.tmux {
        Some(tmux) => {
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            let wrapped = tmux_shell_argv(tmux, &config.shell)?;
            builder = CommandBuilder::new(&tmux.binary);
            // `-u`: the websocket is UTF-8 whatever the server's locale says.
            builder.arg("-u");
            if let Some(file) = &tmux.config_file {
                builder.args(["-f", file]);
            }
            builder.args(["new-session", "-A", "-s", &name]);
            builder.args(&wrapped);
            builder.env_remove("TMUX");
            builder.env_remove("TMUX_PANE");
            session = Some(name);
        }
        None => {
            builder = CommandBuilder::new(&config.shell.program);
            builder.args(&config.shell.args);
        }
    }
    for (key, value) in &config.shell.env {
        builder.env(key, value);
    }
    builder.env("TERM", "xterm-256color");
    builder.env("COLORTERM", "truecolor");
    // PARITY: v4's shell command pops `COLUMNS` and `LINES` so the PTY's size wins.
    builder.env_remove("COLUMNS");
    builder.env_remove("LINES");
    if let Some(cwd) = cwd {
        builder.cwd(cwd);
    }
    Ok((spawn_pty(builder)?, session))
}

/// The environment variables the tmux path does not carry into the shell.
const ENV_SKIP: &[&str] = &[
    "TMUX",
    "TMUX_PANE",
    "TERM",
    "COLUMNS",
    "LINES",
    "SHLVL",
    "_",
    "PWD",
    "OLDPWD",
];

/// Single-quote a value for `sh`.
fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Write the shell's environment for the tmux path (`0600`, replaced atomically) and return its
/// path: the server's environment plus [`ShellCommand::env`].
fn write_env_file(tmux: &TmuxSettings, shell: &ShellCommand) -> std::io::Result<PathBuf> {
    let mut vars: std::collections::BTreeMap<String, String> =
        meerschaum_core::process_env::vars().into_iter().collect();
    for (key, value) in &shell.env {
        vars.insert(key.clone(), value.clone());
    }
    let mut body = String::new();
    for (key, value) in &vars {
        if ENV_SKIP.contains(&key.as_str()) || !valid_env_name(key) {
            continue;
        }
        body.push_str(&format!("{key}={}\n", sh_quote(value)));
    }
    create_private_dir(&tmux.env_dir)?;
    let path = tmux.env_dir.join("webterm.env");
    // Unique per launch: two tabs starting at once must not share a temporary file.
    static LAUNCHES: AtomicUsize = AtomicUsize::new(0);
    let temporary = tmux.env_dir.join(format!(
        "webterm.env.{}.{}.tmp",
        std::process::id(),
        LAUNCHES.fetch_add(1, Ordering::Relaxed)
    ));
    write_private_file(&temporary, body.as_bytes())?;
    std::fs::rename(&temporary, &path)?;
    Ok(path)
}

fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)
}

/// The argv tmux runs in a window: `sh` sources the environment file, then replaces itself with
/// the shell.
fn tmux_shell_argv(tmux: &TmuxSettings, shell: &ShellCommand) -> std::io::Result<Vec<String>> {
    let env_file = write_env_file(tmux, shell)?;
    let mut argv = vec![
        "sh".to_string(),
        "-c".into(),
        "set -a; . \"$0\"; set +a; exec \"$@\"".into(),
        env_file.display().to_string(),
        shell.program.clone(),
    ];
    argv.extend(shell.args.iter().cloned());
    Ok(argv)
}

fn tmux_new_window(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
    session: &str,
) -> std::io::Result<()> {
    let argv = tmux_shell_argv(tmux, shell)?;
    let mut command = std::process::Command::new(&tmux.binary);
    command
        .args(["new-window", "-t", &format!("{session}:")])
        .args(&argv)
        .envs(shell.env.iter().map(|(k, v)| (k, v)))
        .env_remove("TMUX")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    command.status().map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_stream_joins_split_characters() {
        let mut stream = Utf8Stream::default();
        let arrow = "→".as_bytes();
        assert_eq!(stream.push(&arrow[..1]), "");
        assert_eq!(stream.push(&arrow[1..]), "→");
        assert_eq!(stream.push(b"ok\xff!"), "ok\u{FFFD}!");
        assert_eq!(stream.push(&arrow[..2]), "");
        assert_eq!(stream.finish(), "\u{FFFD}");
    }

    #[test]
    fn client_messages_follow_terminado() {
        assert!(matches!(
            parse_client_message(r#"["stdin","ls\r"]"#),
            Some(ClientMessage::Stdin(s)) if s == "ls\r"
        ));
        let Some(ClientMessage::SetSize(size)) =
            parse_client_message(r#"["set_size",30,100,600,800]"#)
        else {
            panic!("set_size");
        };
        assert_eq!(
            (size.rows, size.cols, size.pixel_height, size.pixel_width),
            (30, 100, 600, 800)
        );
        assert!(matches!(
            parse_client_message(r#"["new_window"]"#),
            Some(ClientMessage::NewWindow)
        ));
        for ignored in [
            r#"["set_size",0,10]"#,
            r#"["stdin"]"#,
            r#"{"a":1}"#,
            "nope",
            "[]",
        ] {
            assert!(parse_client_message(ignored).is_none(), "{ignored}");
        }
    }

    #[test]
    fn tmux_session_names_avoid_reserved_characters() {
        assert_eq!(tmux_session_name("a.b:c", "_mrsm"), "a_b_c_mrsm");
    }

    #[test]
    fn readiness_uses_the_exact_live_process_and_actual_buffer_state() {
        let dir = tempfile::tempdir().unwrap();
        let pid = std::process::id();
        let path = dir.path().join(format!("{pid}.json"));
        assert!(read_shell_state(dir.path(), pid).is_none());
        let start = std::fs::read_to_string("/proc/self/stat")
            .ok()
            .and_then(|text| {
                text.rsplit_once(") ")
                    .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                    .map(str::to_string)
            })
            .unwrap_or_default();
        let mut state = json!({"pid": pid, "start": start, "idle": true, "empty": false});
        std::fs::write(&path, state.to_string()).unwrap();
        assert_eq!(
            read_shell_state(dir.path(), pid),
            Some(json!({"idle": true, "empty": false}))
        );
        state["empty"] = json!(true);
        std::fs::write(&path, state.to_string()).unwrap();
        assert_eq!(
            read_shell_state(dir.path(), pid),
            Some(json!({"idle": true, "empty": true}))
        );
        state["idle"] = json!(false);
        std::fs::write(&path, state.to_string()).unwrap();
        assert_eq!(read_shell_state(dir.path(), pid).unwrap()["idle"], false);
        state["pid"] = json!(pid + 1);
        std::fs::write(&path, state.to_string()).unwrap();
        assert!(read_shell_state(dir.path(), pid).is_none());
        #[cfg(target_os = "linux")]
        {
            state["pid"] = json!(pid);
            state["start"] = json!("stale-process");
            std::fs::write(&path, state.to_string()).unwrap();
            assert!(read_shell_state(dir.path(), pid).is_none());
        }
        std::fs::remove_file(&path).unwrap();
        assert!(read_shell_state(dir.path(), pid).is_none());
    }

    #[test]
    fn simultaneous_launches_never_share_an_env_temp_file() {
        let dir = std::env::temp_dir().join(format!("mrsm-webterm-env-{}", std::process::id()));
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            session_suffix: "_t".into(),
            env_dir: dir.clone(),
            config_file: None,
        };
        let shell = ShellCommand::new("sh");
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let (tmux, shell) = (tmux.clone(), shell.clone());
                std::thread::spawn(move || write_env_file(&tmux, &shell))
            })
            .collect();
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
        // Only the final file remains: every temporary was renamed away.
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, ["webterm.env"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn env_values_are_quoted_for_sh() {
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
        assert!(valid_env_name("MRSM_SQL_MAIN"));
        assert!(!valid_env_name("1X"));
        assert!(!valid_env_name("A-B"));
    }
}
