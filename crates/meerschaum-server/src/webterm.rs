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
//!   that is accepted, told why and closed straight away). The terminal needs the permission
//!   `webterm:use` (rc15's access model, [`crate::access`]): administrators, the role
//!   `operator`, and users given it. Under `start api --secure`
//!   (`api:permissions:actions:non_admin: false`) only administrators get a shell. An open
//!   terminal is authorized again every [`REAUTHORIZE_EVERY`]: when the session ends (a logout,
//!   an administrator ending the user's sessions), the user is deleted or loses `webterm:use`,
//!   or the check fails, it says why and closes, and its PTY process ends. That holds while the
//!   browser has stopped reading too: one task writes to the socket, each write bounded, and the
//!   bridge never waits on it.
//! - **tmux.** With `api:webterm:tmux:enabled` (default) and a `tmux` binary, the shell runs in
//!   `tmux new-session -A -s <user><session_suffix>`, so a reconnect (or a second tab) attaches
//!   to the same session. Closing the socket kills the PTY and with it the tmux client, never the
//!   tmux session. Without tmux each connection runs its own shell. The sessions live on a tmux
//!   server of their own: its socket is `tmux.sock` in the webterm resources directory (a `0700`
//!   directory under the root; a short path in the temporary directory when that one is too long
//!   for a socket), never the user's default tmux server, and it starts with Meerschaum's
//!   `tmux.conf` written next to it ([`TMUX_CONF`]: the session's windows as tabs on a status
//!   line at the top, mouse on, a long history, nothing else loaded; applied again when a
//!   terminal attaches to a server that already runs) or the file `api:webterm:tmux:config`
//!   names. A server this process started is stopped with the API ([`stop_owned_servers`]); its
//!   owner is recorded next to the socket ([`tmux_owner_path`]), so the next API on the root
//!   takes over (and later stops) a server whose API was killed before it could. tmux's socket
//!   runs commands for any process of the server's user that connects, and tmux cannot ask for
//!   a secret, so terminals run without tmux (and the server's tmux server is stopped) only
//!   while code a team chose could connect to it ([`WebtermConfig::tmux_refused`]): an isolated
//!   team whose members may install plugins exists, and Unix sockets are not denied to team code
//!   on this server (neither Landlock, Linux 7.1, nor the seccomp filter of older kernels; the
//!   server asks a probe process, [`meerschaum_core::team_scope::sandbox::cached_probe`]).
//! - **Restricted terminals** (5.1). A user who may not run system commands
//!   ([`crate::execute_policy::may_run_system_commands`]: today, anyone who is not an
//!   administrator) gets the same `mrsm` shell in restricted mode ([`restricted`]): every line it
//!   would run is first decided by this server with the decision of `POST /execute` for that
//!   user, it starts no other program, and its tmux sessions live on a separate tmux server with
//!   no key bindings (which follows the same rule: no tmux while isolated teams' sandbox does
//!   not refuse Unix sockets). The mode is decided when the terminal opens and at every check of an open
//!   one: a change of role closes the terminal (and ends the user's tmux session of the other
//!   mode), so the next terminal opens in the new mode, and an unrestricted session is never
//!   reattached by a user who may no longer run system commands.
//! - **The shell** is `<this binary> -i <instance>` (the `mrsm` shell on the server's instance)
//!   started with the server's connection/config environment. Job execution context (including
//!   `MRSM_NONINTERACTIVE`, which every job's environment sets) and `COLUMNS`/`LINES`/`TMUX` are
//!   removed so the shell stays interactive on a job-hosted API.
//!   On the tmux path the environment travels in a `0600` file in the webterm resources
//!   directory that the first shell sources (a pre-existing tmux server would otherwise hand
//!   the shell its own, older, environment, and argv would expose secrets to `ps`).
//! - **Wire protocol** is terminado's: the client sends JSON arrays `["stdin", text]` and
//!   `["set_size", rows, cols, height, width]`; the server sends `["setup", {}]`,
//!   `["stdout", text]` and `["disconnect", 1]`. `["new_window"]` is a v5 addition (tmux only):
//!   it opens a new tmux window running the shell, which v4 did by typing into the terminal (at
//!   most [`MAX_SESSION_WINDOWS`] in a user's session, however often the tab reconnects).
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

mod api_terminal;
mod auth;
mod bridge;
mod guardian;
mod launch;
mod pty;
pub mod remote;
pub mod restricted;
mod session;
pub mod team;
mod tmux;

pub use api_terminal::open_api_terminal;
use auth::*;
pub use auth::{TermMode, executor_routes, origin_allowed, routes};
pub use bridge::active_terminals;
use bridge::*;
use guardian::NativeGuardian;
use launch::*;
pub use launch::{end_executor_terminal, end_tmux_session, tmux_session_name};
use pty::*;
pub use restricted::{
    GRANT_ENV, POLICY_ADDRESS_ENV, Policy, RESTRICTED_ENV, TMUX_RESTRICTED_CONF, restricted_socket,
};
use session::*;
use team::{end_team_sessions_of, start_team_sweeper};
use tmux::*;
pub use tmux::{
    TMUX_CONF, TMUX_WINDOW_LIST, TMUX_WINDOW_NAME, owned_server_sockets, refresh_tmux_config,
    stop_owned_server, stop_owned_servers, tmux_available, tmux_config_path, tmux_owner_path,
    tmux_socket_path,
};

/// Retire only a host-prepared API shell which the caller proves it has never handed to
/// any process launcher. Claimed, malformed or different-owner pending proofs are refused.
/// This is host lifecycle cleanup, not authority derived from a request or shell environment.
pub fn cancel_prepared_api_terminal(shell: &ShellCommand) -> std::io::Result<()> {
    guardian::cancel_unspawned_shell(shell)
}

/// The most terminals one server runs at once (every tab is a process).
pub const MAX_TERMINALS: usize = 128;

/// The largest websocket message accepted from a browser (a paste).
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// Messages waiting for the PTY's writer thread per terminal. A shell that stops reading (or a
/// client that floods it) fills this, and the connection is closed instead of buffering forever.
const INPUT_QUEUE: usize = 64;

/// `["new_window"]` requests one connection may make (each is a process on the tmux path).
const MAX_NEW_WINDOWS: usize = 8;

/// The most windows a user's tmux session may have for `["new_window"]` to open another: windows
/// outlive the connection that opened them, so a reconnect must not start a fresh allowance.
pub const MAX_SESSION_WINDOWS: usize = 16;

/// The websocket close code for "going away / try again later" when [`MAX_TERMINALS`] is reached.
const CLOSE_TRY_AGAIN: u16 = 1013;

/// How often an open terminal authorizes its session again (the session, its user and the
/// user's permission to use the terminal): [`WebtermConfig::reauthorize_every`]'s default.
pub const REAUTHORIZE_EVERY: Duration = Duration::from_secs(10);

/// How long one of those checks may take; a store that does not answer closes the terminal.
const REAUTHORIZE_TIMEOUT: Duration = Duration::from_secs(10);

/// The websocket close code ("policy violation") of a terminal whose access ended while it was
/// open.
const CLOSE_REVOKED: u16 = 1008;

/// What a terminal refused for [`MAX_TERMINALS`] shows.
const TOO_MANY_TERMINALS: &str = "Too many terminals are open on this server. Try again later.";

/// How long one write to a terminal's browser may take. A browser that stops reading for longer
/// is treated as gone; the bridge itself never waits on a write.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a closing terminal lets its last messages (the reason, the close frame) reach the
/// browser, after its PTY process has already been ended.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Messages from the bridge waiting for the socket's writer (`pings`, `pongs`, the closing
/// reason). The backend's output has its own bounded event channel (`remote::QUEUE`).
const CONTROL_QUEUE: usize = 16;

/// The longest a terminal's tmux command may run (counting or opening windows, reading the
/// pane's process, scrubbing the environment); it is killed after that.
const TMUX_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a draining server closes its terminals.
const SHUTTING_DOWN: &str = "server shutting down";

/// How often the thread that reaps a PTY's process checks whether it has exited.
const EXIT_POLL: Duration = Duration::from_millis(200);

/// Variables that make a process part of the job (or the service) the server itself runs in,
/// besides the `static.toml` names [`job_context_vars`] adds. A terminal's shell is never that job:
/// `mrsm` started with `MRSM_SYSTEMD_LOG_PATH` becomes the `systemd` job's wrapper
/// (`meerschaum_jobs::systemd_job`): it takes the job's FIFO as stdin, writes the job's result
/// ("an interactive shell cannot start in a job") and exits, and the terminal shows `[CLOSED]`.
/// A job started from the shell would also carry the server's Compose stamps. Connector, config
/// and authentication variables are kept.
const JOB_CONTEXT_VARS: &[&str] = &[
    // The `systemd` executor's marker for the job's wrapped child.
    "MRSM_SYSTEMD_JOB_CHILD",
    // The local executor's supervisor, its result file, laps and startup gate.
    "MRSM_JOB_RESULT_PATH",
    "MRSM_JOB_SUPERVISOR",
    "MRSM_JOB_PROGRAM",
    "MRSM_JOB_LAPS_PATH",
    "MRSM_JOB_LAPS_PARENT",
    "MRSM_JOB_STARTUP_RELEASE",
    "MRSM_JOB_START_KIND",
    // What the service manager sets for the unit's own process.
    "INVOCATION_ID",
    "JOURNAL_STREAM",
    "NOTIFY_SOCKET",
    "LISTEN_FDS",
    "LISTEN_PID",
    "LISTEN_FDNAMES",
    "WATCHDOG_USEC",
    "WATCHDOG_PID",
    "MANAGERPID",
    "SYSTEMD_EXEC_PID",
    "MEMORY_PRESSURE_WATCH",
    "MEMORY_PRESSURE_WRITE",
];

/// Every variable a terminal's shell must not inherit from the server ([`scrub_job_context`]):
/// the job's name, its `systemd` paths and its Compose stamps (by their `static.toml` names),
/// `MRSM_NONINTERACTIVE` (every job's environment sets it, `meerschaum_jobs::Job::env`; a shell
/// that inherits it reads plain lines without its editor, so it draws no toolbar and never reports
/// an idle prompt to the console's Exec button), `MRSM_NOASK` (a server restarted by an unattended
/// script carries it; a shell that inherits it answers every question with its default, so
/// `python` exits at once and `bootstrap pipes` refuses to ask), and `JOB_CONTEXT_VARS`.
/// A terminal is interactive by definition.
pub fn job_context_vars() -> Vec<String> {
    let environment = &meerschaum_core::static_config::static_config().environment;
    let mut vars: Vec<String> = [
        &environment.daemon_id,
        &environment.systemd_log_path,
        &environment.systemd_result_path,
        &environment.systemd_stdin_path,
        &environment.systemd_delete_job,
        &environment.compose_project,
        &environment.compose_root,
        &environment.noninteractive,
        &environment.noask,
    ]
    .into_iter()
    .cloned()
    .chain(JOB_CONTEXT_VARS.iter().map(|var| var.to_string()))
    .filter(|var| valid_env_name(var))
    .collect();
    vars.sort();
    vars.dedup();
    vars
}

/// Remove the server's job context ([`job_context_vars`]) from a terminal's command, after any
/// explicit overrides: the shell runs as a terminal, never as the job the server runs in.
pub fn scrub_job_context(builder: &mut CommandBuilder) {
    for var in job_context_vars() {
        builder.env_remove(var);
    }
}

static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// The console's session store, as far as the terminal needs it.
#[async_trait]
pub trait SessionLookup: Send + Sync {
    /// The user name logged in under `session_id`; `Ok(None)` for an unknown or expired session.
    async fn username(&self, session_id: &str) -> Result<Option<String>, String>;
    /// Host-selected API profile; its terminal must use the isolated restricted tmux server.
    async fn restricted(&self, _session_id: &str) -> Result<bool, String> {
        Ok(false)
    }
    /// The verified session's username and password binding, captured before remote setup.
    /// Stores without a credential binding cannot authorize a delegated terminal launch.
    async fn binding(&self, _session_id: &str) -> Result<Option<(String, String)>, String> {
        Ok(None)
    }
    /// A private executor may bind its main-owned terminal to a per-launch environment.
    /// Ordinary console session stores retain their configured shell.
    async fn shell(
        &self,
        _session_id: &str,
        configured: &ShellCommand,
    ) -> Result<ShellCommand, String> {
        Ok(configured.clone())
    }
}

/// A [`SessionLookup`] that authorizes no console sessions (for headless/host-API configs and
/// tests that do not serve interactive console logins).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSessions;

#[async_trait]
impl SessionLookup for NoSessions {
    async fn username(&self, _session_id: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
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
    /// A complete execution-context environment; omit inherited variables from PTYs and windows.
    pub clear_env: bool,
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
            clear_env: false,
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

    /// Look up the effective (last) value of `key` in [`Self::env`].
    pub(super) fn env_value(&self, key: &str) -> Option<&str> {
        self.env
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    /// Whether this command requests the API-only execution profile (`MRSM_API_EXECUTION=1`).
    pub(super) fn is_api_execution(&self) -> bool {
        self.env_value(guardian::API_PROFILE_VAR) == Some("1")
    }
}

/// How the tmux path names, creates and wraps sessions.
#[derive(Debug, Clone)]
pub struct TmuxSettings {
    /// The tmux executable.
    pub binary: String,
    /// An explicit tmux server socket (`-S`). Tests always use their own private socket.
    pub socket: Option<PathBuf>,
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
    /// `Some` runs the shell inside a tmux session (unless [`WebtermConfig::tmux_refused`]).
    pub tmux: Option<TmuxSettings>,
    /// The server's root (5.1): while an isolated team that may install plugins is recorded
    /// there and Unix sockets are not denied to team code, terminals run without tmux.
    pub teams_root: Option<PathBuf>,
    /// Whether Unix sockets are denied to isolated teams' code here (by Landlock or the seccomp
    /// filter); `None` asks the server's cached sandbox probe
    /// ([`meerschaum_core::team_scope::sandbox::cached_probe`]), tests fix it.
    pub sandbox_stops_sockets: Option<bool>,
    /// `system.engine.python_team_sandbox`: the mode the probe applies.
    pub team_sandbox_mode: meerschaum_core::team_scope::sandbox::Mode,
    /// How often an open terminal authorizes its session again ([`REAUTHORIZE_EVERY`]).
    pub reauthorize_every: Duration,
    /// The restricted terminals' policy socket and grants ([`restricted`]).
    pub policy: Policy,
    /// Where the policy socket lives (the webterm resources directory).
    pub policy_dir: PathBuf,
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
        let api_shell = shell.is_api_execution();
        let state_dir = if api_shell {
            shell
                .env_value("MRSM_WEBTERM_STATE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| state.config.webterm_resources_path.join("shell-state"))
        } else {
            state.config.webterm_resources_path.join("shell-state")
        };
        let state_ready = if api_shell {
            std::fs::symlink_metadata(&state_dir)
                .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        } else {
            create_private_dir(&state_dir).is_ok()
        };
        if state_ready {
            shell.env.push((
                "MRSM_WEBTERM_STATE_DIR".into(),
                state_dir.to_string_lossy().into_owned(),
            ));
        }
        let tmux = if state.config.webterm_tmux_enabled && tmux_available("tmux", &shell.env) {
            match dedicated_tmux(&state.config) {
                Ok(tmux) => {
                    // A server left behind by a killed API on this root is this one's to stop.
                    TmuxMux::new(tmux.clone(), TmuxProfile::Full).adopt_orphaned_server();
                    Some(tmux)
                }
                Err(error) => {
                    // A terminal without tmux still works; one on the user's own tmux server
                    // must never be started.
                    tracing::warn!(
                        "The web terminal runs without tmux: its tmux socket or configuration \
                         could not be prepared ({error})."
                    );
                    None
                }
            }
        } else {
            None
        };
        let config = WebtermConfig {
            sessions,
            page,
            shell,
            tmux,
            teams_root: Some(state.config.root_dir.clone()),
            sandbox_stops_sockets: None,
            team_sandbox_mode: state.config.team_sandbox_mode,
            reauthorize_every: REAUTHORIZE_EVERY,
            policy: Policy::default(),
            policy_dir: state.config.webterm_resources_path.clone(),
        };
        // The probe runs at start-up when it matters (and again after a team change).
        let _ = config.tmux_refused();
        config
    }

    /// A configuration without a console session store or HTML page renderer (used by private
    /// host-API terminals and tests).
    pub fn headless(state: &AppState, shell: ShellCommand) -> WebtermConfig {
        Self::new(
            state,
            Arc::new(NoSessions),
            Arc::new(|_| String::new()),
            shell,
        )
    }

    /// Why terminals run without tmux now, though it is configured (5.1): an isolated team
    /// whose members may install plugins is recorded (its processes run code the team chose),
    /// and Unix sockets are not denied to team code here (neither Landlock nor the seccomp
    /// filter, as the probe found). Any process of the server's user that connects to tmux's
    /// socket can run commands outside every sandbox, and tmux cannot require a secret; a
    /// terminal without tmux listens on nothing. `None`: tmux may be used (no such team, or
    /// sockets are denied).
    pub fn tmux_refused(&self) -> Option<String> {
        use meerschaum_core::team_scope::{plugin_installing_isolated_teams, sandbox};
        self.tmux.as_ref()?;
        let root = self.teams_root.as_deref()?;
        let teams = plugin_installing_isolated_teams(root);
        if teams.is_empty() {
            return None;
        }
        let (denied, words) = match self.sandbox_stops_sockets {
            Some(denied) => (
                denied,
                sandbox::unix_sockets_words(
                    if denied {
                        sandbox::SOCKETS_BY_SECCOMP
                    } else {
                        ""
                    },
                    None,
                ),
            ),
            None => {
                let report = sandbox::cached_probe(self.team_sandbox_mode, root);
                (report.denies_unix_sockets(), report.sockets_words())
            }
        };
        (!denied).then(|| {
            format!(
                "the isolated team{} {} may install {} own plugins, and on this server {}; \
                 tmux's socket would let that code run commands outside its sandbox",
                if teams.len() == 1 { "" } else { "s" },
                teams
                    .iter()
                    .map(|t| format!("'{t}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if teams.len() == 1 { "its" } else { "their" },
                words
            )
        })
    }

    /// The tmux settings a terminal uses now: [`WebtermConfig::tmux`] unless
    /// [`WebtermConfig::tmux_refused`], in which case the terminal's tmux server is stopped if
    /// this process started it (the refusal is logged once).
    pub fn active_tmux(&self) -> Option<&TmuxSettings> {
        if let Some(why) = self.tmux_refused() {
            static WARNED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::SeqCst) {
                tracing::warn!("The web terminal runs without tmux: {why}.");
            }
            if let Some(socket) = self.tmux.as_ref().and_then(|t| t.socket.as_ref()) {
                stop_owned_server(socket);
                // 5.1: the restricted terminals' and the teams' tmux servers too.
                stop_owned_server(&restricted_socket(socket));
            }
            team::stop_team_servers();
            return None;
        }
        self.tmux.as_ref()
    }

    /// Replace how often an open terminal authorizes its session again (tests shorten it).
    pub fn with_reauthorize_every(mut self, every: Duration) -> WebtermConfig {
        self.reauthorize_every = every;
        self
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

    fn shell_state_directory(&self) -> Option<&str> {
        self.shell.env_value("MRSM_WEBTERM_STATE_DIR")
    }
}

#[cfg(test)]
mod tests;
