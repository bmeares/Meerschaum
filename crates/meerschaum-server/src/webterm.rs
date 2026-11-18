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
mod native_pending;
pub mod remote;
pub mod restricted;
pub use api_terminal::open_api_terminal;

/// Retire only a host-prepared API shell which the caller proves it has never handed to
/// any process launcher. Claimed, malformed or different-owner pending proofs are refused.
/// This is host lifecycle cleanup, not authority derived from a request or shell environment.
pub fn cancel_prepared_api_terminal(shell: &ShellCommand) -> std::io::Result<()> {
    native_pending::cancel_unspawned(shell)
}
pub mod team;

use team::{end_team_sessions_of, launch_team, start_team_sweeper, team_new_window};

pub use restricted::{
    GRANT_ENV, POLICY_ADDRESS_ENV, Policy, RESTRICTED_ENV, TMUX_RESTRICTED_CONF, restricted_socket,
};

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

/// Messages from the bridge waiting for the socket's writer (`setup`, pings, `shell_state`
/// answers, the closing reason). The PTY's output has its own queue ([`spawn_pty`]).
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
        let state_dir = if shell
            .env
            .iter()
            .any(|(key, value)| key == "MRSM_API_EXECUTION" && value == "1")
        {
            shell
                .env
                .iter()
                .rev()
                .find(|(key, _)| key == "MRSM_WEBTERM_STATE_DIR")
                .map(|(_, value)| PathBuf::from(value))
                .unwrap_or_else(|| state.config.webterm_resources_path.join("shell-state"))
        } else {
            state.config.webterm_resources_path.join("shell-state")
        };
        let api_shell = shell
            .env
            .iter()
            .any(|(key, value)| key == "MRSM_API_EXECUTION" && value == "1");
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
                    adopt_orphaned_server(&tmux);
                    Some(tmux)
                }
                Err(error) => {
                    // A terminal without tmux still works; one on the user's own tmux server
                    // must never be started.
                    tracing::warn!(
                        "The web terminal runs without tmux: its tmux socket or configuration                          could not be prepared ({error})."
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
        self.shell
            .env
            .iter()
            .rev()
            .find(|(key, _)| key == "MRSM_WEBTERM_STATE_DIR")
            .map(|(_, value)| value.as_str())
    }
}

/// Meerschaum's tmux configuration for the web terminal's own tmux server. The status line is the
/// session's window list ([`TMUX_WINDOW_LIST`]), at the top of the terminal in the console's
/// colors: one tab per window (the console's `+` opens one), the current one highlighted; a click
/// switches to a window, and `exit` in a window's shell closes it. The shell's own toolbar keeps
/// the bottom row. Only options every tmux release in use knows: an option a server does not know
/// makes tmux show its configuration errors over the first shell.
pub const TMUX_CONF: &str = "\
# The web terminal's tmux configuration, written by Meerschaum. This tmux server runs only the
# web terminal's sessions; set `api:webterm:tmux:config` to the path of your own file instead.
# The session's windows, as tabs on the first row.
set -g status on
set -g status-position top
set -g status-interval 0
set -g status-justify left
set -g status-style \"bg=#17212b,fg=#a8becf\"
set -g status-left \"\"
set -g status-right \"\"
set -g window-status-separator \"\"
set -g window-status-format \" #I #W \"
set -g window-status-current-format \" #I #W \"
set -g window-status-current-style \"bg=#213449,fg=#85caff,bold\"
set -g base-index 1
set -g renumber-windows on
setw -g pane-base-index 1
set -g mouse on
set -g history-limit 50000
set -g default-terminal \"screen-256color\"
set -ga terminal-overrides \",xterm-256color:Tc\"
set -sg escape-time 10
set -g focus-events on
set -g set-titles off
set -g allow-rename off
setw -g aggressive-resize on
";

/// The window list's options, as [`TMUX_CONF`] sets them (`set -g <name> <value>`), which
/// [`refresh_tmux_config`] applies again to a tmux server that already runs.
pub const TMUX_WINDOW_LIST: &[(&str, &str)] = &[
    ("status", "on"),
    ("status-position", "top"),
    ("status-interval", "0"),
    ("status-justify", "left"),
    ("status-style", "bg=#17212b,fg=#a8becf"),
    ("status-left", ""),
    ("status-right", ""),
    ("window-status-separator", ""),
    ("window-status-format", " #I #W "),
    ("window-status-current-format", " #I #W "),
    ("window-status-current-style", "bg=#213449,fg=#85caff,bold"),
    ("base-index", "1"),
    ("renumber-windows", "on"),
];

/// The name of every window the terminal opens (the status line's tab label; a window given a
/// name is never renamed after the program it runs, which is `python` for the Python package).
pub const TMUX_WINDOW_NAME: &str = "mrsm";

/// The longest Unix socket path used as is (`sun_path` holds 104 bytes on macOS, 108 on Linux).
const MAX_SOCKET_PATH: usize = 100;

/// The web terminal's tmux server socket for this root: `tmux.sock` in the webterm resources
/// directory, or (a path too long for a socket) `mrsm-webterm-<user>/<hash of the root>.sock`
/// in the temporary directory. Its directory is created `0700`.
pub fn tmux_socket_path(cfg: &crate::config::ServerConfig) -> std::io::Result<PathBuf> {
    let preferred = cfg.webterm_resources_path.join("tmux.sock");
    // Room for the restricted terminals' socket next to it (`tmux-r.sock`).
    if preferred.as_os_str().len() + 2 <= MAX_SOCKET_PATH {
        create_private_dir(&cfg.webterm_resources_path)?;
        return Ok(preferred);
    }
    use sha2::Digest;
    let digest = sha2::Sha256::digest(cfg.webterm_resources_path.to_string_lossy().as_bytes());
    let hash: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "user".into())
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .collect::<String>();
    let dir = std::env::temp_dir().join(format!("mrsm-webterm-{user}"));
    create_private_dir(&dir)?;
    Ok(dir.join(format!("{hash}.sock")))
}

/// The tmux configuration the terminal's tmux server starts with: `api:webterm:tmux:config`, or
/// Meerschaum's [`TMUX_CONF`] written (`0600`) as `tmux.conf` in the webterm resources directory.
pub fn tmux_config_path(cfg: &crate::config::ServerConfig) -> std::io::Result<PathBuf> {
    if let Some(custom) = &cfg.webterm_tmux_config {
        if !custom.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "`api:webterm:tmux:config` names '{}', which is not a file",
                    custom.display()
                ),
            ));
        }
        return Ok(custom.clone());
    }
    create_private_dir(&cfg.webterm_resources_path)?;
    let path = cfg.webterm_resources_path.join("tmux.conf");
    if std::fs::read_to_string(&path).ok().as_deref() != Some(TMUX_CONF) {
        let temporary = cfg
            .webterm_resources_path
            .join(format!("tmux.conf.{}.tmp", std::process::id()));
        write_private_file(&temporary, TMUX_CONF.as_bytes())?;
        std::fs::rename(&temporary, &path)?;
    }
    Ok(path)
}

/// The tmux settings of a server's terminal: its own tmux server and configuration.
fn dedicated_tmux(cfg: &crate::config::ServerConfig) -> std::io::Result<TmuxSettings> {
    Ok(TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket_path(cfg)?),
        session_suffix: cfg.webterm_tmux_session_suffix.clone(),
        env_dir: cfg.webterm_resources_path.clone(),
        config_file: Some(tmux_config_path(cfg)?.display().to_string()),
    })
}

/// The tmux servers (their sockets) this process started for its terminals.
fn owned_servers() -> &'static std::sync::Mutex<std::collections::BTreeSet<PathBuf>> {
    static OWNED: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeSet<PathBuf>>> =
        std::sync::OnceLock::new();
    OWNED.get_or_init(Default::default)
}

/// The record next to a terminal's tmux socket naming the process (its id) that started the
/// server there and stops it with itself: `tmux.owner` next to `tmux.sock`.
pub fn tmux_owner_path(socket: &Path) -> PathBuf {
    socket.with_extension("owner")
}

/// Whether a tmux server answers on `socket`.
fn tmux_server_running(tmux: &TmuxSettings, socket: &Path) -> bool {
    std::process::Command::new(&tmux.binary)
        .arg("-S")
        .arg(socket)
        .arg("list-sessions")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Before a terminal starts on a dedicated socket: remember that this process starts the tmux
/// server when none answers there yet (so [`stop_owned_servers`] stops it with the API). `true`
/// when a server already answers there.
fn claim_tmux_server(tmux: &TmuxSettings) -> bool {
    let Some(socket) = &tmux.socket else {
        return false;
    };
    if tmux_server_running(tmux, socket) {
        adopt_orphaned_server(tmux);
        return true;
    }
    owned_servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(socket.clone());
    record_owner(socket);
    false
}

/// The process recorded as the owner of the tmux server on `socket` ([`tmux_owner_path`]).
fn recorded_owner(socket: &Path) -> Option<u32> {
    std::fs::read_to_string(tmux_owner_path(socket))
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

/// Record this process as the owner of the tmux server on `socket`.
fn record_owner(socket: &Path) {
    let _ = write_private_file(
        &tmux_owner_path(socket),
        std::process::id().to_string().as_bytes(),
    );
}

/// Whether the process `pid` no longer runs (never this process). Only Unix can tell (tmux
/// runs nowhere else); elsewhere a recorded owner counts as running.
fn owner_gone(pid: u32) -> bool {
    if pid == std::process::id() {
        return false;
    }
    #[cfg(unix)]
    {
        i32::try_from(pid)
            .ok()
            .and_then(rustix::process::Pid::from_raw)
            .is_some_and(|pid| {
                matches!(
                    rustix::process::test_kill_process(pid),
                    Err(rustix::io::Errno::SRCH)
                )
            })
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Take over the tmux server on the terminal's own socket when the API that started it is gone
/// (killed, so it never stopped it): this process then stops it with itself
/// ([`stop_owned_servers`]), and its sessions stay until then. A server whose recorded owner
/// still runs (another API on the same root) or that has no record (started by an earlier
/// release) is left alone. Called when the terminal is configured, so a server left behind is
/// stopped even when no terminal opens, and again before each terminal starts. `true` when
/// adopted.
fn adopt_orphaned_server(tmux: &TmuxSettings) -> bool {
    let Some(socket) = &tmux.socket else {
        return false;
    };
    let Some(owner) = recorded_owner(socket) else {
        return false;
    };
    if !owner_gone(owner) || !tmux_server_running(tmux, socket) {
        return false;
    }
    owned_servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(socket.clone());
    record_owner(socket);
    true
}

/// Apply Meerschaum's window list ([`TMUX_WINDOW_LIST`]) again to a tmux server that already runs
/// on the terminal's own socket (one `tmux -S <socket> set-option -g ... ; ...`): a server started
/// by an earlier release (whose configuration turned the status line off), or left behind by an
/// API that did not stop cleanly, keeps the options it started with otherwise. Only plain
/// `set-option -g`s: applying them again changes nothing. A file of the user's own
/// (`api:webterm:tmux:config`) is theirs to apply; nothing happens without a dedicated socket.
pub fn refresh_tmux_config(tmux: &TmuxSettings) -> bool {
    let (Some(socket), Some(file)) = (&tmux.socket, &tmux.config_file) else {
        return false;
    };
    if std::fs::read_to_string(file).ok().as_deref() != Some(TMUX_CONF) {
        return false;
    }
    let mut command = std::process::Command::new(&tmux.binary);
    command.arg("-S").arg(socket);
    for (i, (name, value)) in TMUX_WINDOW_LIST.iter().enumerate() {
        if i > 0 {
            command.arg(";");
        }
        command.args(["set-option", "-g", name, value]);
    }
    command.env_remove("TMUX").env_remove("TMUX_PANE");
    tmux_output(&mut command).is_some_and(|output| output.status.success())
}

/// Unset the server's job context ([`job_context_vars`]) on the terminal's own tmux server (`-S`,
/// never the default server): in its global environment and in the user's `session` (exact name),
/// either of which a server started by an earlier release, from a job's environment or with an
/// `update-environment` setting may hold. Windows opened afterwards, with the terminal's
/// `["new_window"]` or with tmux's own keys, start without it. Nothing happens when no server
/// answers or the session does not exist yet.
fn scrub_tmux_server_env(tmux: &TmuxSettings, session: Option<&str>) {
    let Some(socket) = &tmux.socket else {
        return;
    };
    let tmux_command = || {
        let mut command = std::process::Command::new(&tmux.binary);
        command
            .arg("-S")
            .arg(socket)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        command
    };
    let context = job_context_vars();
    // `=name`: exactly this session, never one whose name merely starts with it.
    let target = session.map(|name| format!("={name}"));
    let mut scopes: Vec<Vec<&str>> = vec![vec!["-g"]];
    if let Some(target) = &target {
        scopes.push(vec!["-t", target.as_str()]);
    }
    for scope in scopes {
        let Some(listing) = tmux_output(tmux_command().arg("show-environment").args(&scope)) else {
            return;
        };
        if !listing.status.success() {
            continue;
        }
        let present: Vec<String> = String::from_utf8_lossy(&listing.stdout)
            .lines()
            .filter_map(|line| line.split_once('=').map(|(name, _)| name.to_string()))
            .filter(|name| context.contains(name))
            .collect();
        if present.is_empty() {
            continue;
        }
        // One tmux invocation: `set-environment <scope> -u A ; set-environment <scope> -u B ...`.
        let mut command = tmux_command();
        for (i, name) in present.iter().enumerate() {
            if i > 0 {
                command.arg(";");
            }
            command
                .arg("set-environment")
                .args(&scope)
                .args(["-u", name.as_str()]);
        }
        let _ = tmux_output(&mut command);
    }
}

/// Run a tmux command within [`TMUX_TIMEOUT`] (killed when it takes longer): its output, or
/// `None` when it could not start or did not finish in time. Every tmux command a terminal runs
/// while it is open goes through here, so a hung tmux server never holds a terminal's tasks.
fn tmux_output(command: &mut std::process::Command) -> Option<std::process::Output> {
    let mut child = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + TMUX_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

/// Stop the tmux servers this process started for its web terminals (`tmux -S <socket>
/// kill-server` on each, never the default server). The API calls it when it stops; a tmux
/// server it found already running is left alone.
pub fn stop_owned_servers() {
    let sockets: Vec<PathBuf> =
        std::mem::take(&mut *owned_servers().lock().unwrap_or_else(|e| e.into_inner()))
            .into_iter()
            .collect();
    for socket in sockets {
        kill_tmux_server(&socket);
    }
}

/// Stop the one tmux server on `socket` if this process started it; `false` if it did not.
pub fn stop_owned_server(socket: &Path) -> bool {
    let owned = owned_servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(socket);
    if owned {
        kill_tmux_server(socket);
    }
    owned
}

fn kill_tmux_server(socket: &Path) {
    let _ = std::process::Command::new("tmux")
        .arg("-S")
        .arg(socket)
        .arg("kill-server")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    if recorded_owner(socket) == Some(std::process::id()) {
        let _ = std::fs::remove_file(tmux_owner_path(socket));
    }
}

/// The sockets of the tmux servers this process started (for tests).
pub fn owned_server_sockets() -> Vec<PathBuf> {
    owned_servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .cloned()
        .collect()
}

/// Whether `binary` runs (`tmux -V`).
pub fn tmux_available(binary: &str, env: &[(String, String)]) -> bool {
    std::process::Command::new(binary)
        .arg("-V")
        .envs(env.iter().map(|(k, v)| (k, v)))
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
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
    executor: bool,
}

/// The `/webterm/{session_id}` and `/websocket/{session_id}` routes. Merge them only when the
/// terminal is served (`!no_webterm`, the console is on); without them both paths answer `404`.
pub fn routes(state: AppState, config: WebtermConfig) -> Router {
    Router::new()
        .route("/webterm/{session_id}", get(page))
        .route("/webterm/{session_id}/", get(page))
        .route("/websocket/{session_id}", get(socket))
        .route("/websocket/{session_id}/", get(socket))
        .with_state(Inner {
            state,
            config,
            executor: false,
        })
}

/// Private executor websocket only. Requires signed gateway identity and leased sessions;
/// it never interprets a main-console session UUID or permits the ordinary no-auth bypass.
pub fn executor_routes(state: AppState, config: WebtermConfig) -> Router {
    Router::new()
        .route("/websocket/{session_id}", get(socket))
        .with_state(Inner {
            state,
            config,
            executor: true,
        })
}

/// How a terminal's shell runs (5.1, [`crate::execute_policy::may_run_system_commands`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermMode {
    /// The full `mrsm` shell on the server (administrators).
    Full,
    /// The restricted shell: every line decided by the server ([`restricted`]).
    Restricted,
    /// A team terminal: an `mrsm` shell that is itself a process of the team (its configuration,
    /// the environment allowlist and the sandbox applied before the shell starts), on a tmux
    /// server of the team's own that runs nothing else ([`team::team_tmux`]).
    Team(String),
    /// A main-owned terminal transported to one immutable team executor identity.
    Remote(uuid::Uuid),
    /// A bounded API-only shell on Main, bound to its registered primary team or Everyone.
    HostApi(Option<uuid::Uuid>),
}

/// The user a session may open a terminal as, or why not.
enum Access {
    /// The terminal's name (the user's), how its shell runs, and the team record of a team
    /// terminal.
    Granted {
        name: String,
        mode: TermMode,
        team: Option<Box<meerschaum_core::access::Team>>,
    },
    /// No session (log in).
    Denied,
    /// A session whose user may not use the terminal (`webterm:use`).
    Forbidden,
    Unavailable,
    /// A member of an isolated team whose team terminal is not available here, and why.
    NoTeamTerminal(String),
}

impl Access {
    /// What a refused websocket shows before it closes (v4 closed it without a word, leaving a
    /// blank terminal).
    fn refusal(&self) -> Option<String> {
        match self {
            Access::Granted { .. } => None,
            Access::Denied => Some(
                "Not logged in, or the session has ended. Log in again to use the terminal.".into(),
            ),
            Access::Forbidden => Some("You do not have permission to use the terminal.".into()),
            Access::Unavailable => Some("Could not check your session. Try again later.".into()),
            Access::NoTeamTerminal(reason) => Some(team_terminal_refusal(reason)),
        }
    }

    /// Why an open terminal of `term_name` (started in `mode`) closes after authorizing its
    /// session again; `None` keeps it open.
    fn revocation(&self, term_name: &str, mode: &TermMode) -> Option<String> {
        match self {
            Access::Granted {
                name, mode: now, ..
            } if name == term_name => (now != mode).then(|| ROLE_CHANGED.to_string()),
            Access::Granted { .. } | Access::Denied => {
                Some("Your session has ended. Log in again to use the terminal.".into())
            }
            Access::Forbidden => Some("Your permission to use the terminal was removed.".into()),
            Access::Unavailable => {
                Some("Could not verify your session, so the terminal was closed.".into())
            }
            Access::NoTeamTerminal(reason) => Some(team_terminal_refusal(reason)),
        }
    }
}

/// What a member of an isolated team hears when their team terminal is not available.
fn team_terminal_refusal(reason: &str) -> String {
    format!("Your team's terminal is not available on this server: {reason}.")
}

/// Who `session_id` may open a terminal as. `fresh` reads the user's identity now instead of
/// from the access cache (a websocket, and every check of an open one).
async fn authorize(inner: &Inner, session_id: &str, fresh: bool) -> Access {
    if inner.executor {
        let Some(caller) = crate::executor::gateway::current() else {
            return Access::Denied;
        };
        let name = match inner.config.sessions.username(session_id).await {
            Ok(Some(name)) => name,
            Ok(None) => return Access::Denied,
            Err(_) => return Access::Unavailable,
        };
        return match inner.config.sessions.restricted(session_id).await {
            Ok(restricted) => Access::Granted {
                name,
                mode: if restricted {
                    TermMode::Restricted
                } else {
                    TermMode::Team(caller.claims.team_id.to_string())
                },
                team: None,
            },
            Err(_) => Access::Unavailable,
        };
    }
    // PARITY: `is_session_authenticated` is true for everyone under `--no-auth`; the terminal is
    // named after the session then (`username or session_id`).
    if inner.state.config.no_auth {
        return Access::Granted {
            name: session_id.to_string(),
            mode: TermMode::Full,
            team: None,
        };
    }
    let username = match inner.config.sessions.username(session_id).await {
        Ok(Some(username)) if !username.is_empty() => username,
        Ok(_) => return Access::Denied,
        Err(error) => {
            tracing::warn!("The session store failed for a terminal: {error}");
            return Access::Unavailable;
        }
    };
    authorize_user(&inner.state, &username, fresh).await
}

/// Who `username` may open a terminal as (the part of [`authorize`] after the session).
async fn authorize_user(state: &AppState, username: &str, fresh: bool) -> Access {
    let username = username.to_string();
    // FIX (intentional v4 change, rc15 access model): v4 gave every logged-in user a shell
    // unless `--secure`; the terminal now needs `webterm:use` (administrators, the role
    // `operator`, or the permission given), and `--secure` keeps it for administrators. v4 also
    // asked only the session store: the session's user must still be a user of the instance.
    let user = meerschaum_core::users::User::new(&username, None);
    match state.instance().get_user_id(&user).await {
        Ok(Some(id)) if !id.is_null() => {}
        Ok(_) => return Access::Denied,
        Err(error) => {
            tracing::warn!("Could not read the user of a terminal session: {error}");
            return Access::Unavailable;
        }
    }
    let principal = crate::auth::Principal::User(user);
    let granted = crate::auth::Scopes::List(vec!["*".into()]);
    let access = if fresh {
        crate::access::access_of_fresh(state, principal, &granted).await
    } else {
        crate::access::access_of(state, principal, &granted).await
    };
    let access = match access {
        Ok(access) => access,
        Err(e) if e.status.as_u16() == 503 => return Access::Unavailable,
        Err(_) => return Access::Denied,
    };
    if !access.is_admin() && (!state.config.actions_non_admin || !access.has("webterm:use")) {
        return Access::Forbidden;
    }
    let selected_remote = (!access.full_admin())
        .then(|| {
            access
                .team_records
                .iter()
                .find(|team| {
                    meerschaum_core::access::same_team(Some(&team.name), Some(&access.primary_team))
                        && access.in_team(Some(&team.name))
                        && access.principal.permits_team(Some(&team.name))
                })
                .cloned()
        })
        .flatten()
        .filter(|team| {
            team.execution()
                .is_ok_and(|settings| settings.mode.is_remote())
        });
    if let Some(team) =
        selected_remote.or_else(|| crate::team_execution::code_running_team(&access))
        && team.execution().is_ok_and(|s| s.mode.is_remote())
    {
        if !["webterm:use", "actions:execute", "jobs:execute"]
            .iter()
            .all(|p| access.has(p))
        {
            return Access::Forbidden;
        }
        let Some(id) = team
            .id
            .as_deref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
        else {
            return Access::NoTeamTerminal("the selected team has no registered identity".into());
        };
        return Access::Granted {
            name: username,
            mode: TermMode::Remote(id),
            team: Some(Box::new(team)),
        };
    }
    // 5.1: the one answer to "may this user run system commands" decides the shell's mode. A
    // member of an isolated team gets their team's terminal or none: never a shell in this
    // server's process.
    use crate::execute_policy::SystemCommands;
    match crate::execute_policy::may_run_system_commands(state, &access).await {
        SystemCommands::Anywhere => Access::Granted {
            name: username,
            mode: TermMode::Full,
            team: None,
        },
        SystemCommands::TeamOnly { team } => match access.acting_team(None) {
            Some(record) if meerschaum_core::access::same_team(Some(&record.name), Some(&team)) => {
                Access::Granted {
                    name: username,
                    mode: TermMode::Team(team),
                    team: Some(Box::new(record)),
                }
            }
            _ => Access::NoTeamTerminal("the team you act for changed".into()),
        },
        SystemCommands::No { .. } => {
            if !["webterm:use", "actions:execute", "jobs:execute"]
                .iter()
                .all(|permission| access.has(permission))
            {
                return Access::Forbidden;
            }
            let team_id = if meerschaum_core::access::is_everyone(Some(&access.primary_team)) {
                None
            } else {
                let Some(id) = access
                    .team_records
                    .iter()
                    .find(|team| {
                        meerschaum_core::access::same_team(
                            Some(&team.name),
                            Some(&access.primary_team),
                        )
                    })
                    .and_then(|team| team.id.as_deref())
                    .and_then(|value| uuid::Uuid::parse_str(value).ok())
                    .filter(|id| !id.is_nil())
                else {
                    return Access::NoTeamTerminal(
                        "the primary team has no current registered identity".into(),
                    );
                };
                Some(id)
            };
            Access::Granted {
                name: username,
                mode: TermMode::HostApi(team_id),
                team: None,
            }
        }
    }
}

/// Why an open terminal closes when its user's role changed (they may now run system commands,
/// or no longer may).
const ROLE_CHANGED: &str = "Your role changed. Open the terminal again.";

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

/// The page of a signed-in user without the permission `webterm:use`.
const FORBIDDEN_PAGE: &str = r#"<html>
    <head>
        <style>
        body { background-color: #222222; }
        #message-div { padding: 15px; font-size: x-large; font-family: sans-serif; }
        h2 { color: #FFFFFF; }
        p { color: #FFFFFF; }
        </style>
    </head>
    <body>
        <div id="message-div">
            <h2>No terminal access</h2>
            <p>Ask an administrator for the permission to use the terminal.</p>
        </div>
    </body>
</html>
"#;

/// The page of a member of an isolated team whose team terminal is not available.
fn no_team_terminal_page(reason: &str) -> String {
    let escaped = reason
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    FORBIDDEN_PAGE
        .replace("No terminal access", "No terminal for your team")
        .replace(
            "Ask an administrator for the permission to use the terminal.",
            &format!("Your team's terminal is not available on this server: {escaped}."),
        )
}

async fn page(State(inner): State<Inner>, UrlPath(session_id): UrlPath<String>) -> Response {
    let mut response = match authorize(&inner, &session_id, false).await {
        Access::Granted { .. } => Html((inner.config.page)(&session_id)).into_response(),
        Access::Denied => (StatusCode::UNAUTHORIZED, Html(UNAUTHORIZED_PAGE)).into_response(),
        Access::Forbidden => (StatusCode::FORBIDDEN, Html(FORBIDDEN_PAGE)).into_response(),
        Access::NoTeamTerminal(reason) => {
            (StatusCode::FORBIDDEN, Html(no_team_terminal_page(&reason))).into_response()
        }
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
    verified: Option<axum::Extension<crate::executor::gateway::VerifiedRequest>>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !origin_allowed(&headers) {
        return (StatusCode::FORBIDDEN, "Cross-origin websocket refused.").into_response();
    }
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let verified = inner
                .executor
                .then(|| verified.map(|axum::Extension(caller)| caller))
                .flatten();
            crate::executor::gateway::scope(verified, async move {
                // PARITY: v4 accepts the websocket, then closes it when the session is not authorized
                // (v5 says why first).
                match authorize(&inner, &session_id, true).await {
                    Access::Granted { name, mode, team } => {
                        run(inner, session_id, name, mode, team, socket).await
                    }
                    refused => {
                        let reason = refused.refusal().unwrap_or_default();
                        refuse(socket, &reason, 1000).await
                    }
                }
            })
            .await;
        })
}

fn close_message(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}

/// Send one message, giving up after [`WRITE_TIMEOUT`]; `false` when it was not sent.
async fn send_within<S>(sink: &mut S, message: Message) -> bool
where
    S: futures_util::Sink<Message> + Unpin,
{
    matches!(
        tokio::time::timeout(WRITE_TIMEOUT, sink.send(message)).await,
        Ok(Ok(()))
    )
}

/// The terminal's line for `reason` (the `[CLOSED]` that follows is the browser's).
fn reason_message(reason: &str) -> Message {
    text(json!(["stdout", format!("\r\n[{reason}]\r\n")]))
}

/// Tell the browser why, end the terminal (`["disconnect", 1]`, so it does not reconnect) and
/// close the socket with `code`, each write bounded.
async fn refuse(mut socket: WebSocket, reason: &str, code: u16) {
    if send_within(&mut socket, reason_message(reason)).await
        && send_within(&mut socket, text(json!(["disconnect", 1]))).await
    {
        let _ = send_within(&mut socket, close_message(code, "")).await;
    }
}

/// Queue messages for the socket's writer without waiting: a full queue (a browser that stopped
/// reading) drops them.
fn queue(control: &mpsc::Sender<Message>, messages: impl IntoIterator<Item = Message>) {
    for message in messages {
        let _ = control.try_send(message);
    }
}

/// How a terminal's socket writer ended.
enum WriterEnd {
    /// The PTY closed (the shell exited): its last output, `["disconnect", 1]` and a close frame
    /// were sent.
    ShellExited,
    /// It sent the bridge's close frame, or the bridge let go of it.
    Closed,
    /// A write failed or did not finish within [`WRITE_TIMEOUT`] (the browser is gone or stopped
    /// reading).
    PeerGone,
}

/// The only task that writes to a terminal's socket: the bridge's messages (`control`, first)
/// and the PTY's output, each write bounded by [`WRITE_TIMEOUT`]. A browser that stops reading
/// stalls only this task; the bridge keeps reading input and checking the session.
async fn write_terminal(
    mut tx: futures_util::stream::SplitSink<WebSocket, Message>,
    mut output: mpsc::Receiver<Vec<u8>>,
    mut control: mpsc::Receiver<Message>,
) -> WriterEnd {
    let mut decoder = Utf8Stream::default();
    loop {
        tokio::select! {
            biased;
            message = control.recv() => {
                let Some(message) = message else {
                    return WriterEnd::Closed;
                };
                let close = matches!(message, Message::Close(_));
                if !send_within(&mut tx, message).await {
                    return WriterEnd::PeerGone;
                }
                if close {
                    return WriterEnd::Closed;
                }
            }
            chunk = output.recv() => match chunk {
                Some(bytes) => {
                    let out = decoder.push(&bytes);
                    if !out.is_empty()
                        && !send_within(&mut tx, text(json!(["stdout", out]))).await
                    {
                        return WriterEnd::PeerGone;
                    }
                }
                None => {
                    // Each write waits only while the previous one went through.
                    let rest = decoder.finish();
                    if (rest.is_empty()
                        || send_within(&mut tx, text(json!(["stdout", rest]))).await)
                        && send_within(&mut tx, text(json!(["disconnect", 1]))).await
                    {
                        let _ = send_within(&mut tx, close_message(1000, "")).await;
                    }
                    return WriterEnd::ShellExited;
                }
            },
        }
    }
}

fn text(parts: Value) -> Message {
    Message::Text(parts.to_string().into())
}

/// Bridge one websocket to one PTY until either side ends, or the session's access does.
async fn run(
    inner: Inner,
    session_id: String,
    term_name: String,
    mode: TermMode,
    team: Option<Box<meerschaum_core::access::Team>>,
    mut socket: WebSocket,
) {
    let restricted = mode == TermMode::Restricted;
    let mut config = inner.config.clone();
    let guard = match TerminalGuard::acquire() {
        Some(guard) => guard,
        None => {
            refuse(socket, TOO_MANY_TERMINALS, CLOSE_TRY_AGAIN).await;
            return;
        }
    };
    let _guard = guard;
    if let TermMode::HostApi(team_id) = mode {
        remote::bridge_host(inner, session_id, term_name, team_id, socket).await;
        return;
    }
    if let TermMode::Remote(team_id) = mode {
        if let Some(team) = team {
            remote::bridge(inner, session_id, term_name, team_id, *team, socket).await;
        } else {
            refuse(
                socket,
                "The terminal's executor placement is unavailable.",
                1011,
            )
            .await;
        }
        return;
    }
    if inner.executor {
        match config.sessions.shell(&session_id, &config.shell).await {
            Ok(shell) => config.shell = shell,
            Err(_) => {
                refuse(
                    socket,
                    "This terminal's execution environment is unavailable.",
                    1011,
                )
                .await;
                return;
            }
        }
    }
    // A restricted shell asks this server's policy socket before it runs anything.
    let policy_address = if restricted && inner.executor {
        Ok(Some(restricted::API_POLICY_ADDRESS.to_string()))
    } else if restricted {
        config
            .policy
            .listen(&inner.state, &config.policy_dir)
            .await
            .map(Some)
    } else {
        Ok(None)
    };
    // A team terminal's shell is a process of the team (5.1), from the host of the actions.
    let team_shell = match (&mode, &team) {
        (TermMode::Team(_), None) if inner.executor => Some(Ok(config.shell.clone())),
        (TermMode::Team(_), Some(record)) => Some(
            inner
                .state
                .actions
                .team_shell(record)
                .map_err(std::io::Error::other),
        ),
        (TermMode::Team(_), None) => Some(Err(std::io::Error::other("no team record"))),
        _ => None,
    };
    let launch = match (policy_address, team_shell) {
        (_, Some(Err(error))) => Ok(Err(error)),
        (_, Some(Ok(shell))) => {
            let config = config.clone();
            let name = term_name.clone();
            let team_name = match &mode {
                TermMode::Team(team) => team.clone(),
                _ => String::new(),
            };
            let state = inner.state.clone();
            let every = config.reauthorize_every;
            // Factory lease expiry retires its own sessions, including detached windows.
            let check_membership = !inner.executor;
            let launched = tokio::task::spawn_blocking(move || {
                launch_team(&config, &name, &team_name, &shell, check_membership)
            })
            .await;
            if check_membership && matches!(launched, Ok(Ok(_))) {
                start_team_sweeper(&state, every);
            }
            launched
        }
        (Ok(address), None) => {
            let config = config.clone();
            let name = term_name.clone();
            tokio::task::spawn_blocking(move || launch(&config, &name, address.as_deref())).await
        }
        (Err(error), None) => Ok(Err(error)),
    };
    let Launched {
        pty,
        session: tmux_session,
        tmux: active_tmux,
        shell: active_shell,
        grant: _grant,
        team_window,
    } = match launch {
        Ok(Ok(started)) => started,
        Ok(Err(error)) => {
            tracing::warn!("Failed to start a terminal for '{term_name}': {error}");
            let reason = text(json!([
                "stdout",
                format!("Failed to start the shell: {error}\r\n")
            ]));
            if send_within(&mut socket, reason).await
                && send_within(&mut socket, text(json!(["disconnect", 1]))).await
            {
                let _ = send_within(&mut socket, close_message(1011, "")).await;
            }
            return;
        }
        Err(_) => return,
    };
    let Pty {
        process_id,
        master,
        input,
        output,
        killer,
    } = pty;
    let (tx, mut rx) = socket.split();
    // Every write goes through one task with bounded writes, so nothing here ever waits on a
    // browser that stopped reading: input, the session's checks and the teardown stay live.
    let (control, control_rx) = mpsc::channel::<Message>(CONTROL_QUEUE);
    queue(&control, [text(json!(["setup", {}]))]);
    let mut writer = tokio::spawn(write_terminal(tx, output, control_rx));
    let mut writer_done = false;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await;
    // The session, its user and the permission are checked again while the terminal is open: a
    // logout, a deleted user or a removed permission must not leave a shell running.
    let every = config.reauthorize_every.max(Duration::from_millis(10));
    let mut recheck = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut closed_by_client = false;
    let mut new_windows = 0usize;
    // At most one `shell_state` query runs per connection (each may run tmux). Requests that
    // arrive meanwhile are coalesced: the latest waits for the next query, an older one is
    // answered at once without a snapshot (the browser's probe treats that as "unknown").
    let mut shell_job: Option<tokio::task::JoinHandle<Option<Value>>> = None;
    let mut shell_request: Option<String> = None;
    let mut shell_waiting: Option<String> = None;
    // A draining server closes the terminal at once (`1001 Going Away`): the process on the PTY
    // ends (a tmux session survives it), and the browser may reconnect once a server is back.
    let drain = inner.state.lifecycle.signal().wait();
    tokio::pin!(drain);
    loop {
        tokio::select! {
            () = &mut drain => {
                queue(&control, [
                    reason_message(SHUTTING_DOWN),
                    close_message(1001, SHUTTING_DOWN),
                ]);
                break;
            }
            snapshot = joined(&mut shell_job), if shell_job.is_some() => {
                shell_job = None;
                let snapshot = snapshot.ok().flatten();
                if let Some(request) = shell_request.take() {
                    queue(&control, [text(json!(["shell_state", request, snapshot]))]);
                }
                if let Some(request) = shell_waiting.take() {
                    let session = tmux_session.as_deref();
                    shell_job = Some(spawn_shell_state(
                        &config,
                        active_tmux.as_ref(),
                        session,
                        process_id,
                    ));
                    shell_request = Some(request);
                }
            }
            ended = &mut writer, if !writer_done => {
                // The shell exited (its last output and close frame are sent), or the browser is
                // gone or stopped reading.
                writer_done = true;
                if matches!(ended, Ok(WriterEnd::PeerGone)) {
                    tracing::debug!("A browser stopped reading the terminal of '{term_name}'.");
                }
                break;
            }
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
                                queue(&control, [
                                    text(json!(["stdout", "\r\n[input queue full]\r\n"])),
                                    text(json!(["disconnect", 1])),
                                    close_message(1000, ""),
                                ]);
                                break;
                            }
                        }
                        Some(ClientMessage::SetSize(size)) => {
                            let _ = master.resize(size);
                        }
                        Some(ClientMessage::NewWindow) => {
                            // Each window is a process: bounded per connection and per user
                            // session (`MAX_SESSION_WINDOWS`, across reconnects), and refused
                            // while the server is at its terminal limit. tmux runs off the
                            // bridge (and bounded): it never delays input or a revocation.
                            // A restricted terminal's window runs its restricted shell on the
                            // restricted tmux server (`active_tmux`, `active_shell`).
                            if new_windows < MAX_NEW_WINDOWS
                                && active_terminals() < MAX_TERMINALS
                                && let (Some(settings), Some(session)) =
                                    (&active_tmux, &tmux_session)
                                && config.tmux_refused().is_none()
                            {
                                new_windows += 1;
                                let (settings, shell, session) =
                                    (settings.clone(), active_shell.clone(), session.clone());
                                tokio::task::spawn_blocking(move || {
                                    if team_window {
                                        team_new_window(&settings, &shell, &session)
                                    } else {
                                        tmux_new_window(&settings, &shell, &session)
                                    }
                                });
                            }
                        }
                        Some(ClientMessage::ShellState(request)) => {
                            if shell_job.is_none() {
                                let session = tmux_session.as_deref();
                                shell_job = Some(spawn_shell_state(
                                    &config,
                                    active_tmux.as_ref(),
                                    session,
                                    process_id,
                                ));
                                shell_request = Some(request);
                            } else if let Some(dropped) = shell_waiting.replace(request) {
                                queue(&control, [text(json!(["shell_state", dropped, null]))]);
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
                // A browser that is gone or stopped reading is found by the writer's bounded
                // writes.
                queue(&control, [Message::Ping(Vec::new().into())]);
            }
            _ = recheck.tick() => {
                // Fail closed: an error or a store that does not answer ends the terminal too.
                let access = tokio::time::timeout(
                    REAUTHORIZE_TIMEOUT,
                    authorize(&inner, &session_id, true),
                )
                .await
                .unwrap_or(Access::Unavailable);
                if let Some(reason) = access.revocation(&term_name, &mode) {
                    tracing::info!("Closing the terminal of '{term_name}': {reason}");
                    // A changed role ends the user's tmux session of the old mode too: an
                    // unrestricted shell cannot be restricted after the fact, so a demoted user
                    // never reattaches one (and its windows and commands end now). A team
                    // terminal's session ends with any revocation (5.1).
                    if (reason == ROLE_CHANGED || team_window)
                        && let (Some(settings), Some(session)) = (&active_tmux, &tmux_session)
                    {
                        let (settings, session, policy) =
                            (settings.clone(), session.clone(), config.policy.clone());
                        tokio::task::spawn_blocking(move || {
                            end_tmux_session(&settings, &session);
                            policy.forget_session(&session);
                        });
                    }
                    queue(&control, [
                        reason_message(&reason),
                        text(json!(["disconnect", 1])),
                        close_message(CLOSE_REVOKED, &reason),
                    ]);
                    break;
                }
            }
        }
    }
    // End the process on the PTY (the tmux client, never the tmux session) first, whether or not
    // the browser ever reads the reason, then close the input queue: the writer thread drops its
    // writer only after the process has exited, so the EOF it sends reaches no one (see
    // `spawn_pty`).
    drop(killer);
    drop(input);
    // A `shell_state` query still running is this connection's: stop it (its tmux command is
    // bounded by `TMUX_TIMEOUT` either way).
    if let Some(job) = shell_job.take() {
        job.abort();
    }
    // Then let the socket's writer deliver what was queued (the reason, the close frame), for a
    // bounded time.
    drop(control);
    if !writer_done
        && (closed_by_client
            || tokio::time::timeout(CLOSE_TIMEOUT, &mut writer)
                .await
                .is_err())
    {
        writer.abort();
    }
}

/// The result of an in-flight job; never ready without one.
async fn joined<T>(
    job: &mut Option<tokio::task::JoinHandle<T>>,
) -> Result<T, tokio::task::JoinError> {
    match job {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

/// Read the shell's readiness off the bridge (it may run tmux, bounded by `TMUX_TIMEOUT`).
fn spawn_shell_state(
    config: &WebtermConfig,
    tmux: Option<&TmuxSettings>,
    session: Option<&str>,
    process_id: Option<u32>,
) -> tokio::task::JoinHandle<Option<Value>> {
    let (config, tmux, session) = (config.clone(), tmux.cloned(), session.map(str::to_string));
    tokio::task::spawn_blocking(move || {
        shell_state(&config, tmux.as_ref(), session.as_deref(), process_id)
    })
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

/// Ends the PTY's process when dropped: the thread that reaps the process signals it.
struct KillOnDrop(std::sync::mpsc::Sender<()>);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

fn pty_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

fn spawn_pty(builder: CommandBuilder, state_dir: Option<PathBuf>) -> std::io::Result<Pty> {
    let pair = native_pty_system()
        .openpty(PtySize::default())
        .map_err(pty_error)?;
    let mut child = pair.slave.spawn_command(builder).map_err(pty_error)?;
    let process_id = child.process_id();
    let readiness = state_dir.and_then(|dir| process_id.map(|pid| dir.join(format!("{pid}.json"))));
    // The child holds the slave end now; ours must close for EOF to reach the reader.
    drop(pair.slave);
    let (kill_tx, kill_rx) = std::sync::mpsc::channel::<()>();
    let killer = KillOnDrop(kill_tx);
    // Reap the child whenever it ends, and tell the writer thread (see below). When the bridge
    // lets go of it (`KillOnDrop`), end it here, on the only thread that reaps it, so a signal
    // never reaches a reused PID: SIGHUP, then SIGKILL after a short grace (portable-pty's
    // `Child::kill`), so a process that ignores SIGHUP ends too.
    let (exited_tx, exited_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        loop {
            match kill_rx.recv_timeout(EXIT_POLL) {
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if !matches!(child.try_wait(), Ok(None)) {
                        break;
                    }
                }
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = ChildKiller::kill(&mut *child);
                    let _ = child.wait();
                    break;
                }
            }
        }
        // A disconnected plain terminal kills its shell before its own guard can clean up.
        if let Some(path) = readiness {
            let _ = std::fs::remove_file(path);
        }
        let _ = exited_tx.send(());
    });
    // (An error from here on drops `killer`, which ends the child.)
    let mut reader = pair.master.try_clone_reader().map_err(pty_error)?;
    let mut writer = pair.master.take_writer().map_err(pty_error)?;
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
    tmux: Option<&TmuxSettings>,
    session: Option<&str>,
    process_id: Option<u32>,
) -> Option<Value> {
    let dir = config.shell_state_directory()?;
    let pid = if let (Some(tmux), Some(session)) = (tmux, session) {
        let mut command = std::process::Command::new(&tmux.binary);
        if let Some(socket) = &tmux.socket {
            command.arg("-S").arg(socket);
        }
        // `=`: exactly this terminal's session, never another whose name starts with it.
        let target = format!("={session}:");
        let output = tmux_output(
            command
                .args(["display-message", "-p", "-t", &target, "#{pane_pid}"])
                .envs(config.shell.env.iter().map(|(k, v)| (k, v)))
                .env_remove("TMUX")
                .env_remove("TMUX_PANE"),
        )?;
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
    let pid = if config
        .shell
        .env
        .iter()
        .any(|(key, value)| key == "MRSM_API_EXECUTION" && value == "1")
    {
        let spec = config
            .shell
            .env
            .iter()
            .rev()
            .find(|(key, _)| key == "MRSM_TEAM_SANDBOX")
            .map(|(_, value)| Path::new(value))?;
        meerschaum_jobs::supervisor::terminal_child_pid(i32::try_from(pid).ok()?, spec)?
            .try_into()
            .ok()?
    } else {
        pid
    };
    read_shell_state(Path::new(dir), pid)
}

const MAX_SHELL_STATE_BYTES: u64 = 1024 * 1024;

/// Hold the selected directory while opening its numeric leaf: a confined shell may replace
/// its own state entries, but status reads must never follow links or wait on a FIFO/device.
fn shell_state_file(dir: &Path, pid: u32) -> Option<std::fs::File> {
    #[cfg(unix)]
    {
        use rustix::fs::{Mode, OFlags, open, openat};
        let directory = open(
            dir,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .ok()?;
        let leaf = openat(
            &directory,
            format!("{pid}.json"),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
            Mode::empty(),
        )
        .ok()?;
        Some(std::fs::File::from(leaf))
    }
    #[cfg(not(unix))]
    {
        // Legacy non-Unix terminals retain their status display with a bounded regular file.
        // API-only execution profiles are unavailable on these platforms.
        let directory = std::fs::symlink_metadata(dir).ok()?;
        if !directory.is_dir() || directory.file_type().is_symlink() {
            return None;
        }
        let path = dir.join(format!("{pid}.json"));
        let leaf = std::fs::symlink_metadata(&path).ok()?;
        if !leaf.is_file() || leaf.file_type().is_symlink() {
            return None;
        }
        std::fs::File::open(path).ok()
    }
}

fn read_shell_state(dir: &Path, pid: u32) -> Option<Value> {
    let file = shell_state_file(dir, pid)?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > MAX_SHELL_STATE_BYTES {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(MAX_SHELL_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_SHELL_STATE_BYTES {
        return None;
    }
    let snapshot: Value = serde_json::from_slice(&bytes).ok()?;
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

/// The tmux session name for a terminal: tmux rejects `.` and `:` in names, so they are written
/// `_d` and `_c`, and `_` itself `__` (5.1: one user's session name is never another's, as
/// `a.b` and `a_b` both were `a_b`). The suffix (`api:webterm:tmux:session_suffix`, the same for
/// everyone) keeps the old replacement.
pub fn tmux_session_name(term_name: &str, suffix: &str) -> String {
    let mut name = String::with_capacity(term_name.len() + suffix.len());
    for c in term_name.chars() {
        match c {
            '_' => name.push_str("__"),
            '.' => name.push_str("_d"),
            ':' => name.push_str("_c"),
            c => name.push(c),
        }
    }
    name.push_str(&suffix.replace(['.', ':'], "_"));
    name
}

/// A started terminal.
struct Launched {
    pty: Pty,
    /// The tmux session it attached to (the tmux path).
    session: Option<String>,
    /// The tmux server that session lives on (the restricted one for a restricted terminal).
    tmux: Option<TmuxSettings>,
    /// The shell a new window runs (a restricted terminal's carries its grant).
    shell: ShellCommand,
    /// A restricted terminal's own grant on the plain path (one shell per connection).
    grant: Option<restricted::GrantGuard>,
    /// A team terminal (5.1): its windows run the team's shell from an empty environment.
    team_window: bool,
}

/// The tmux settings of the restricted terminals next to `tmux` (the administrators'): their own
/// socket ([`restricted_socket`]) and Meerschaum's [`TMUX_RESTRICTED_CONF`] (never the
/// administrators' `api:webterm:tmux:config`). `None` without a dedicated socket: the default
/// tmux server is never used, so such a terminal runs without tmux.
fn restricted_tmux(tmux: &TmuxSettings) -> Option<TmuxSettings> {
    let socket = tmux.socket.as_ref()?;
    Some(TmuxSettings {
        binary: tmux.binary.clone(),
        socket: Some(restricted_socket(socket)),
        session_suffix: tmux.session_suffix.clone(),
        env_dir: tmux.env_dir.clone(),
        config_file: Some(
            tmux.env_dir
                .join("tmux-restricted.conf")
                .display()
                .to_string(),
        ),
    })
}

/// `shell` in restricted mode: it asks `address` with `grant` before running anything.
fn restricted_shell(shell: &ShellCommand, address: &str, grant: &str) -> ShellCommand {
    let mut shell = shell.clone();
    shell.env.retain(|(key, _)| {
        ![RESTRICTED_ENV, POLICY_ADDRESS_ENV, GRANT_ENV].contains(&key.as_str())
    });
    shell.env.push((RESTRICTED_ENV.into(), "1".into()));
    shell.env.push((POLICY_ADDRESS_ENV.into(), address.into()));
    shell.env.push((GRANT_ENV.into(), grant.into()));
    // The shell may be `python -m meerschaum`: never an interactive interpreter after it exits,
    // a startup script or a debugger.
    for (var, value) in RESTRICTED_PYTHON_VARS {
        shell.env.retain(|(key, _)| key != var);
        shell.env.push((var.to_string(), value.to_string()));
    }
    shell
}

/// What a restricted shell's Python variables are, whatever the server's environment says: they
/// would make a Python interpreter (the shell, when it runs as `python -m meerschaum`) stay
/// interactive, run a script of its own or open a debugger (an empty value is unset for Python;
/// `PYTHONBREAKPOINT=0` turns `breakpoint()` off).
const RESTRICTED_PYTHON_VARS: &[(&str, &str)] = &[
    ("PYTHONINSPECT", ""),
    ("PYTHONSTARTUP", ""),
    ("PYTHONBREAKPOINT", "0"),
];

/// A tmux command on `tmux`'s own socket (never the default server), without the caller's tmux
/// variables or the server's job context.
fn tmux_command(tmux: &TmuxSettings) -> Option<std::process::Command> {
    let socket = tmux.socket.as_ref()?;
    let mut command = std::process::Command::new(&tmux.binary);
    command
        .arg("-S")
        .arg(socket)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE");
    for var in job_context_vars() {
        command.env_remove(var);
    }
    Some(command)
}

/// Whether the tmux `session` (exact name) exists on `tmux`'s socket.
fn tmux_has_session(tmux: &TmuxSettings, session: &str) -> bool {
    let Some(mut command) = tmux_command(tmux) else {
        return false;
    };
    command.args(["has-session", "-t", &format!("={session}")]);
    tmux_output(&mut command).is_some_and(|output| output.status.success())
}

/// Retire all panes of the exact Factory lease on its Team, API-only or legacy socket.
/// Each socket belongs to this executor configuration. A dedicated base is required.
pub fn end_executor_terminal(base: &TmuxSettings, team_id: uuid::Uuid, name: &str) {
    let session = tmux_session_name(name, &base.session_suffix);
    for tmux in [
        team::team_tmux(base, &team_id.to_string()),
        restricted_tmux(base),
    ]
    .into_iter()
    .flatten()
    {
        end_tmux_session(&tmux, &session);
    }
    end_tmux_session(base, &session);
}

/// End the tmux `session` (exact name) on `tmux`'s socket, with every window and process in it.
/// Nothing happens without a dedicated socket, a running server or such a session.
pub fn end_tmux_session(tmux: &TmuxSettings, session: &str) {
    if let Some(mut command) = tmux_command(tmux) {
        command.args(["kill-session", "-t", &format!("={session}")]);
        let _ = tmux_output(&mut command);
    }
}

/// Write [`TMUX_RESTRICTED_CONF`] (`0600`) where `tmux` (restricted settings) reads it.
fn write_restricted_conf(tmux: &TmuxSettings) -> std::io::Result<()> {
    let Some(file) = &tmux.config_file else {
        return Err(std::io::Error::other(
            "no restricted tmux configuration path",
        ));
    };
    let path = Path::new(file);
    if std::fs::read_to_string(path).ok().as_deref() == Some(TMUX_RESTRICTED_CONF) {
        return Ok(());
    }
    create_private_dir(&tmux.env_dir)?;
    let temporary = tmux
        .env_dir
        .join(format!("tmux-restricted.conf.{}.tmp", std::process::id()));
    write_private_file(&temporary, TMUX_RESTRICTED_CONF.as_bytes())?;
    std::fs::rename(&temporary, path)
}

/// Lock down the restricted tmux server that runs on `tmux`'s socket: apply
/// [`TMUX_RESTRICTED_CONF`] (`source-file`), empty every key table it does not keep
/// ([`restricted::KEPT_TABLES`]: tmux has more tables than the ones the file names, and new
/// releases add some), then check that tmux lists exactly the file's bindings and no prefix
/// key. `true` only when all of that holds.
fn apply_restricted_conf(tmux: &TmuxSettings) -> bool {
    let Some(file) = &tmux.config_file else {
        return false;
    };
    let run = |args: &[&str]| -> Option<std::process::Output> {
        let mut command = tmux_command(tmux)?;
        command.args(args);
        tmux_output(&mut command).filter(|output| output.status.success())
    };
    if run(&["source-file", file]).is_none() {
        return false;
    }
    let Some(listing) = run(&["list-keys"]) else {
        return false;
    };
    let mut tables: Vec<String> = String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter_map(|line| restricted::binding(line).map(|(table, _, _)| table))
        .filter(|table| !restricted::KEPT_TABLES.contains(&table.as_str()))
        .collect();
    tables.sort();
    tables.dedup();
    for table in &tables {
        if run(&["unbind-key", "-a", "-T", table]).is_none() {
            return false;
        }
    }
    let Some(listing) = run(&["list-keys"]) else {
        return false;
    };
    if let Err(error) = restricted::only_allowed_bindings(&String::from_utf8_lossy(&listing.stdout))
    {
        tracing::warn!("A restricted terminal's tmux server is not locked down: {error}");
        return false;
    }
    ["prefix", "prefix2"].iter().all(|option| {
        run(&["show-options", "-gv", option])
            .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "None")
    })
}

/// Start a restricted terminal's tmux session `name` for `term_name` on `tmux` (restricted
/// A complete shell environment is delivered in its private pane file. The tmux client
/// and server run as the terminal host and resolve their tools with the host environment.
fn configure_tmux_environment(command: &mut std::process::Command, shell: &ShellCommand) {
    if shell.clear_env {
        command.env_clear().envs(team::minimal_env());
    } else {
        command.envs(shell.env.iter().map(|(key, value)| (key, value)));
    }
}

/// settings), detached; the terminal attaches to it afterwards. Its grant.
fn new_restricted_session(
    config: &WebtermConfig,
    tmux: &TmuxSettings,
    term_name: &str,
    name: &str,
    address: &str,
) -> std::io::Result<String> {
    let grant = config.policy.grant_for_session(term_name, name);
    let shell = restricted_shell(&config.shell, address, &grant);
    let argv = tmux_shell_argv(tmux, &shell)?;
    let mut command =
        tmux_command(tmux).ok_or_else(|| std::io::Error::other("no restricted tmux socket"))?;
    configure_tmux_environment(&mut command, &shell);
    command.arg("-u");
    if let Some(file) = &tmux.config_file {
        command.args(["-f", file]);
    }
    command
        .args(["new-session", "-d", "-s", name, "-n", TMUX_WINDOW_NAME])
        .args(&argv)
        .env("TERM", "xterm-256color")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE");
    if let Some(cwd) = shell.cwd.clone().or_else(|| std::env::current_dir().ok()) {
        command.current_dir(cwd);
    }
    for var in job_context_vars() {
        command.env_remove(var);
    }
    if tmux_output(&mut command).is_some_and(|output| output.status.success()) {
        Ok(grant)
    } else {
        config.policy.forget_session(name);
        Err(std::io::Error::other(
            "tmux could not start the restricted session",
        ))
    }
}

/// Start a terminal's process: the shell itself, or tmux around it (`new-session -A`, or for a
/// restricted terminal a session on the restricted tmux server, then `attach-session`).
/// `restricted` is the policy socket's address when the terminal is restricted.
fn launch(
    config: &WebtermConfig,
    term_name: &str,
    restricted: Option<&str>,
) -> std::io::Result<Launched> {
    let cwd = match &config.shell.cwd {
        Some(dir) => Some(dir.clone()),
        None => std::env::current_dir().ok(),
    };
    let mut builder;
    let mut session = None;
    let mut active_tmux = None;
    let mut grant = None;
    let mut shell = config.shell.clone();
    // 5.1: no tmux at all while isolated teams' sandbox does not refuse Unix sockets
    // (`WebtermConfig::active_tmux`), for restricted terminals too.
    match (config.active_tmux(), restricted) {
        (Some(tmux), None) => {
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            // A user who may run system commands now has no use for a restricted session left
            // from before (a promotion): end it.
            if let Some(restricted_tmux) = restricted_tmux(tmux)
                && tmux_has_session(&restricted_tmux, &name)
            {
                end_tmux_session(&restricted_tmux, &name);
                config.policy.forget_session(&name);
            }
            let wrapped = tmux_shell_argv(tmux, &config.shell)?;
            if claim_tmux_server(tmux) {
                refresh_tmux_config(tmux);
            }
            scrub_tmux_server_env(tmux, Some(&name));
            builder = CommandBuilder::new(&tmux.binary);
            if let Some(socket) = &tmux.socket {
                builder.arg("-S");
                builder.arg(socket);
            }
            // `-u`: the websocket is UTF-8 whatever the server's locale says.
            builder.arg("-u");
            if let Some(file) = &tmux.config_file {
                builder.args(["-f", file]);
            }
            // `-n`: the first window's tab label (ignored when the session exists already).
            builder.args(["new-session", "-A", "-s", &name, "-n", TMUX_WINDOW_NAME]);
            builder.args(&wrapped);
            session = Some(name);
            active_tmux = Some(tmux.clone());
        }
        (Some(tmux), Some(address)) if restricted_tmux(tmux).is_some() => {
            let rtmux = restricted_tmux(tmux).expect("checked");
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            // One restricted launch at a time: two tabs opening at once must not both start the
            // session (the second one's grant would replace the first's).
            static RESTRICTED_LAUNCH: std::sync::Mutex<()> = std::sync::Mutex::new(());
            let _launching = RESTRICTED_LAUNCH.lock().unwrap_or_else(|e| e.into_inner());
            // A user who may not run system commands never reattaches a session started when
            // they could (a demotion): end it, with whatever runs in it.
            end_tmux_session(tmux, &name);
            write_restricted_conf(&rtmux)?;
            claim_tmux_server(&rtmux);
            // The server already runs: lock it down before anything else happens on it.
            if tmux_server_running(&rtmux, rtmux.socket.as_deref().expect("socket"))
                && !apply_restricted_conf(&rtmux)
            {
                return Err(std::io::Error::other(
                    "the restricted tmux configuration could not be applied",
                ));
            }
            scrub_tmux_server_env(&rtmux, Some(&name));
            // Reattach only a session whose shell this server started for this user (so its
            // grant is known); end any other.
            let token = match config.policy.session_grant(term_name, &name) {
                Some(token) if tmux_has_session(&rtmux, &name) => {
                    // This fresh prepared shell has not been handed to argv construction or
                    // any launcher. Reattachment keeps the existing pane's claimed proof.
                    native_pending::cancel_unspawned(&config.shell)?;
                    token
                }
                _ => {
                    end_tmux_session(&rtmux, &name);
                    new_restricted_session(config, &rtmux, term_name, &name, address)?
                }
            };
            // tmux applies all of it, or the terminal does not open.
            if !apply_restricted_conf(&rtmux) {
                end_tmux_session(&rtmux, &name);
                config.policy.forget_session(&name);
                return Err(std::io::Error::other(
                    "the restricted tmux configuration could not be applied",
                ));
            }
            shell = restricted_shell(&config.shell, address, &token);
            builder = CommandBuilder::new(&rtmux.binary);
            builder.arg("-S");
            builder.arg(rtmux.socket.as_ref().expect("socket"));
            builder.arg("-u");
            builder.args(["attach-session", "-t", &format!("={name}")]);
            session = Some(name);
            active_tmux = Some(rtmux);
        }
        (_, Some(address)) => {
            // One restricted shell per connection, with a grant that ends with it.
            let guard = config.policy.grant_for_connection(term_name);
            shell = restricted_shell(&config.shell, address, guard.token());
            grant = Some(guard);
            builder = CommandBuilder::new(&shell.program);
            builder.args(&shell.args);
        }
        (None, None) => {
            builder = CommandBuilder::new(&config.shell.program);
            builder.args(&config.shell.args);
        }
    }
    if shell.clear_env {
        builder.env_clear();
    }
    if shell.clear_env && active_tmux.is_some() {
        for (key, value) in team::minimal_env() {
            builder.env(key, value);
        }
    } else {
        for (key, value) in &shell.env {
            builder.env(key, value);
        }
    }
    // A caller's shell overrides must not reconnect a scratch or web terminal to the
    // enclosing user's tmux server. Strip these after applying the overrides too.
    builder.env_remove("TMUX");
    builder.env_remove("TMUX_PANE");
    // Nor run the shell as the job the server runs in (the tmux client's environment also becomes
    // a new tmux server's global environment).
    scrub_job_context(&mut builder);
    builder.env("TERM", "xterm-256color");
    builder.env("COLORTERM", "truecolor");
    // PARITY: v4's shell command pops `COLUMNS` and `LINES` so the PTY's size wins.
    builder.env_remove("COLUMNS");
    builder.env_remove("LINES");
    if let Some(cwd) = cwd {
        builder.cwd(cwd);
    }
    let state_dir = if active_tmux.is_none() {
        config.shell_state_directory().map(PathBuf::from)
    } else {
        // The PTY owns a tmux client; the session's shell survives that client's exit.
        None
    };
    // A user who may now run commands this way never reattaches a team terminal of before.
    if let Some(name) = &session {
        end_team_sessions_of(name);
    }
    Ok(Launched {
        pty: spawn_pty(builder, state_dir)?,
        session,
        tmux: active_tmux,
        shell,
        grant,
        team_window: false,
    })
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
    let mut vars: std::collections::BTreeMap<String, String> = if shell.clear_env {
        Default::default()
    } else {
        meerschaum_core::process_env::vars().into_iter().collect()
    };
    for (key, value) in &shell.env {
        vars.insert(key.clone(), value.clone());
    }
    let context = job_context_vars();
    let mut body = String::new();
    for (key, value) in &vars {
        if ENV_SKIP.contains(&key.as_str()) || !valid_env_name(key) || context.contains(key) {
            continue;
        }
        body.push_str(&format!("{key}={}\n", sh_quote(value)));
    }
    create_private_dir(&tmux.env_dir)?;
    remove_stale_env_files(&tmux.env_dir);
    // One file per launch, which the window's `sh` removes once it has read it
    // ([`tmux_shell_argv`]): two terminals starting at once never read each other's (a
    // restricted terminal's carries its own grant), and none stays behind.
    static LAUNCHES: AtomicUsize = AtomicUsize::new(0);
    let stem = format!(
        "webterm.{}.{}",
        std::process::id(),
        LAUNCHES.fetch_add(1, Ordering::Relaxed)
    );
    let path = tmux.env_dir.join(format!("{stem}.env"));
    let temporary = tmux.env_dir.join(format!("{stem}.env.tmp"));
    write_private_file(&temporary, body.as_bytes())?;
    std::fs::rename(&temporary, &path)?;
    Ok(path)
}

/// How long an environment file may wait for its window before it counts as abandoned (tmux
/// never started the window).
const STALE_ENV_FILE: Duration = Duration::from_secs(120);

/// Remove the environment files ([`write_env_file`]) no window read: older than
/// [`STALE_ENV_FILE`], and the shared `webterm.env` of earlier releases.
fn remove_stale_env_files(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("webterm.") || !(name.ends_with(".env") || name.ends_with(".tmp")) {
            continue;
        }
        let stale = name == "webterm.env"
            || entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > STALE_ENV_FILE);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
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

/// The argv tmux runs in a window: `sh` sources the environment file and removes it, unsets the
/// server's job context (a tmux server started before, or from a job's environment, may hold it
/// in its global environment), then replaces itself with the shell. A file that cannot be read
/// ends `sh` there (`.` is a special built-in): the window closes, nothing else runs.
fn tmux_shell_argv(tmux: &TmuxSettings, shell: &ShellCommand) -> std::io::Result<Vec<String>> {
    let env_file = write_env_file(tmux, shell)?;
    // The names are `valid_env_name`s: safe to write into the script as they are.
    let unset = job_context_vars().join(" ");
    let mut argv: Vec<String> = if shell.clear_env {
        vec!["/usr/bin/env".into(), "-i".into()]
    } else {
        Vec::new()
    };
    argv.extend([
        "sh".to_string(),
        "-c".into(),
        format!("set -a; . \"$0\"; set +a; command -p rm -f -- \"$0\"; unset {unset}; exec \"$@\""),
        env_file.display().to_string(),
        shell.program.clone(),
    ]);
    argv.extend(shell.args.iter().cloned());
    Ok(argv)
}

/// How many windows the tmux `session` (exact name) has; `None` when tmux cannot say.
fn tmux_window_count(tmux: &TmuxSettings, shell: &ShellCommand, session: &str) -> Option<usize> {
    let mut command = std::process::Command::new(&tmux.binary);
    configure_tmux_environment(&mut command, shell);
    if let Some(socket) = &tmux.socket {
        command.arg("-S").arg(socket);
    }
    let output = tmux_output(
        command
            .args([
                "list-windows",
                "-t",
                &format!("={session}"),
                "-F",
                "#{window_id}",
            ])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE"),
    )?;
    output.status.success().then(|| {
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
    })
}

/// Open a window running the shell in the user's tmux `session` (exact name), unless it already
/// has [`MAX_SESSION_WINDOWS`] (or its windows cannot be counted): `Ok(false)` then.
fn tmux_new_window(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
    session: &str,
) -> std::io::Result<bool> {
    // One at a time, so two tabs of a session cannot both pass the count.
    static OPENING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _opening = OPENING.lock().unwrap_or_else(|e| e.into_inner());
    if tmux_window_count(tmux, shell, session).is_none_or(|n| n >= MAX_SESSION_WINDOWS) {
        return Ok(false);
    }
    let shell = native_pending::window_shell(shell)?;
    let argv = tmux_shell_argv(tmux, &shell)?;
    scrub_tmux_server_env(tmux, Some(session));
    let mut command = std::process::Command::new(&tmux.binary);
    configure_tmux_environment(&mut command, &shell);
    if let Some(socket) = &tmux.socket {
        command.arg("-S").arg(socket);
    }
    // `=`: exactly this session, never another whose name starts with it.
    command
        .args([
            "new-window",
            "-t",
            &format!("={session}:"),
            "-n",
            TMUX_WINDOW_NAME,
        ])
        .args(&argv)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for var in job_context_vars() {
        command.env_remove(var);
    }
    Ok(tmux_output(&mut command).is_some_and(|output| output.status.success()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminals_never_carry_the_servers_job_context() {
        let vars = job_context_vars();
        for name in [
            "MRSM_SYSTEMD_LOG_PATH",
            "MRSM_SYSTEMD_RESULT_PATH",
            "MRSM_SYSTEMD_STDIN_PATH",
            "MRSM_SYSTEMD_DELETE_JOB",
            "MRSM_SYSTEMD_JOB_CHILD",
            "MRSM_DAEMON_ID",
            "MRSM__COMPOSE_PROJECT",
            "MRSM__COMPOSE_ROOT",
            "MRSM_JOB_RESULT_PATH",
            "MRSM_JOB_SUPERVISOR",
            "INVOCATION_ID",
            "JOURNAL_STREAM",
            // Every job's environment sets it; the terminal's shell would lose its editor.
            "MRSM_NONINTERACTIVE",
            // An unattended restart's environment; the shell would answer no question.
            "MRSM_NOASK",
        ] {
            assert!(vars.iter().any(|var| var == name), "{name} must be removed");
        }
        // The shell's own configuration stays.
        for name in [
            "MRSM_ROOT_DIR",
            "MRSM_PLUGINS_DIR",
            "MRSM_CONFIG",
            "MRSM_PATCH",
            "MRSM_SQL_MAIN",
            "MRSM_PYTHON",
            "PATH",
            "HOME",
        ] {
            assert!(!vars.iter().any(|var| var == name), "{name} must be kept");
        }
        assert!(vars.iter().all(|var| valid_env_name(var)));

        let mut builder = CommandBuilder::new("sh");
        builder.env("MRSM_SYSTEMD_LOG_PATH", "/job/log");
        builder.env("MRSM_DAEMON_ID", "api");
        builder.env("MRSM_SQL_MAIN", "sqlite:///main.db");
        scrub_job_context(&mut builder);
        assert!(builder.get_env("MRSM_SYSTEMD_LOG_PATH").is_none());
        assert!(builder.get_env("MRSM_DAEMON_ID").is_none());
        assert!(builder.get_env("MRSM_SQL_MAIN").is_some());
    }

    #[test]
    fn the_tmux_window_unsets_the_job_context_and_its_env_file_omits_it() {
        let dir = tempfile::tempdir().unwrap();
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: None,
            session_suffix: String::new(),
            env_dir: dir.path().join("env"),
            config_file: None,
        };
        let mut shell = ShellCommand::new("mrsm");
        // Explicit overrides are scrubbed too; ordinary ones are written.
        shell.env.push(("MRSM_DAEMON_ID".into(), "api".into()));
        shell
            .env
            .push(("MRSM_SYSTEMD_LOG_PATH".into(), "/job/log".into()));
        shell.env.push(("MRSM_RC25_KEEP".into(), "yes".into()));
        let argv = tmux_shell_argv(&tmux, &shell).unwrap();
        assert!(argv[2].contains("unset "), "{}", argv[2]);
        for var in job_context_vars() {
            assert!(
                argv[2]
                    .split(' ')
                    .any(|word| word.trim_end_matches(';') == var),
                "{var}"
            );
        }
        assert!(argv[2].ends_with("exec \"$@\""), "{}", argv[2]);
        let body = std::fs::read_to_string(&argv[3]).unwrap();
        assert!(body.contains("MRSM_RC25_KEEP='yes'"), "{body}");
        for var in job_context_vars() {
            assert!(
                !body
                    .lines()
                    .any(|line| line.starts_with(&format!("{var}="))),
                "{var}"
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn an_isolated_tmux_window_uses_only_its_supplied_environment() {
        let root = tempfile::tempdir().unwrap();
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: None,
            session_suffix: String::new(),
            env_dir: root.path().join("env"),
            config_file: None,
        };
        let mut shell = ShellCommand::new("/bin/sh");
        shell.clear_env = true;
        shell.env = vec![
            ("PATH".into(), "/bin:/usr/bin".into()),
            ("TPR_CURRENT_CONTEXT".into(), "current".into()),
        ];
        shell.args = vec!["-c".into(), "env".into()];
        let argv = tmux_shell_argv(&tmux, &shell).unwrap();
        // Exercise the actual window wrapper without starting any tmux server.
        let output = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .env(
                "TPR_UNEXPECTED_PARENT_CREDENTIAL",
                "synthetic-parent-secret",
            )
            .output()
            .unwrap();
        assert!(output.status.success());
        let environment = String::from_utf8(output.stdout).unwrap();
        assert!(
            environment
                .lines()
                .any(|line| line == "TPR_CURRENT_CONTEXT=current")
        );
        assert!(!environment.contains("TPR_UNEXPECTED_PARENT_CREDENTIAL"));
        assert!(!environment.contains("synthetic-parent-secret"));
        assert!(!environment.contains("CARGO_MANIFEST_DIR"));
    }

    #[test]
    fn a_restricted_shell_carries_the_servers_restriction_and_no_python_escape() {
        let mut shell = ShellCommand::new("mrsm");
        // Whatever the server's own command carries is replaced.
        shell.env.push((RESTRICTED_ENV.into(), "".into()));
        shell.env.push((GRANT_ENV.into(), "old".into()));
        shell.env.push(("PYTHONINSPECT".into(), "1".into()));
        let restricted = restricted_shell(&shell, "unix:/p.sock", "g");
        let get = |key: &str| -> Vec<&str> {
            restricted
                .env
                .iter()
                .filter(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
                .collect()
        };
        assert_eq!(get(RESTRICTED_ENV), ["1"]);
        assert_eq!(get(POLICY_ADDRESS_ENV), ["unix:/p.sock"]);
        assert_eq!(get(GRANT_ENV), ["g"]);
        assert_eq!(get("PYTHONINSPECT"), [""]);
        assert_eq!(get("PYTHONSTARTUP"), [""]);
        assert_eq!(get("PYTHONBREAKPOINT"), ["0"]);
        // Restricted sessions never use the default tmux server.
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: None,
            session_suffix: String::new(),
            env_dir: PathBuf::from("/e"),
            config_file: None,
        };
        assert!(restricted_tmux(&tmux).is_none());
        let tmux = TmuxSettings {
            socket: Some(PathBuf::from("/w/tmux.sock")),
            ..tmux
        };
        let restricted = restricted_tmux(&tmux).unwrap();
        assert_eq!(restricted.socket, Some(PathBuf::from("/w/tmux-r.sock")));
        assert_eq!(
            restricted.config_file.as_deref(),
            Some("/e/tmux-restricted.conf")
        );
    }

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
    fn tmux_session_names_avoid_reserved_characters_and_never_collide() {
        assert_eq!(tmux_session_name("a.b:c", "_mrsm"), "a_db_cc_mrsm");
        assert_eq!(tmux_session_name("alice", "_mrsm"), "alice_mrsm");
        // Two users never share a session (`a.b` and `a_b` both were `a_b`).
        let names = ["a.b", "a_b", "a__b", "a_db", "a:b", "a_.b", "a._b"];
        let mut sessions: Vec<String> =
            names.iter().map(|n| tmux_session_name(n, "_s.x")).collect();
        assert!(
            sessions
                .iter()
                .all(|s| !s.contains('.') && !s.contains(':'))
        );
        sessions.sort();
        sessions.dedup();
        assert_eq!(sessions.len(), names.len());
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

    fn current_readiness_snapshot() -> Value {
        let pid = std::process::id();
        let start = std::fs::read_to_string("/proc/self/stat")
            .ok()
            .and_then(|text| {
                text.rsplit_once(") ")?
                    .1
                    .split_whitespace()
                    .nth(19)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        json!({"pid":pid,"start":start,"idle":true,"empty":true})
    }

    #[cfg(unix)]
    #[test]
    fn readiness_refuses_outside_file_and_directory_symlinks() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        std::fs::create_dir(&state).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let pid = std::process::id();
        let name = format!("{pid}.json");
        std::fs::write(
            outside.join(&name),
            current_readiness_snapshot().to_string(),
        )
        .unwrap();
        assert_eq!(
            read_shell_state(&outside, pid),
            Some(json!({"idle":true,"empty":true}))
        );
        symlink(outside.join(&name), state.join(&name)).unwrap();
        assert!(read_shell_state(&state, pid).is_none());
        std::fs::remove_file(state.join(&name)).unwrap();
        std::fs::remove_dir(&state).unwrap();
        symlink(&outside, &state).unwrap();
        assert!(read_shell_state(&state, pid).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn readiness_refuses_a_named_fifo_promptly_without_reading_it() {
        use rustix::fs::{CWD, Mode, mkfifoat};
        const CHILD: &str = "MRSM_TEST_SHELL_STATE_FIFO";
        if let Some(directory) = std::env::var_os(CHILD) {
            let directory = Path::new(&directory);
            let leaf = directory.join(format!("{}.json", std::process::id()));
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::fs::symlink_metadata(&leaf).is_err() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "The owned FIFO was not prepared."
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(read_shell_state(directory, std::process::id()).is_none());
            std::fs::write(directory.join("finished"), "1").unwrap();
            return;
        }
        let root = tempfile::tempdir().unwrap();
        // The child waits for this owned leaf. Parent-side timeout can kill an accidental
        // blocking open/read without leaving a stuck test thread or FIFO writer behind.
        let name = format!(
            "{}::readiness_refuses_a_named_fifo_promptly_without_reading_it",
            module_path!()
        );
        let name = name.split_once("::").unwrap().1;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--test-threads=1"])
            .env_clear()
            .env(CHILD, root.path())
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", root.path().join("plugins"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let path = root.path().join(format!("{}.json", child.id()));
        // The child waits for its own numeric leaf before entering the status reader.
        mkfifoat(CWD, &path, Mode::RUSR | Mode::WUSR).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let result = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            result.is_some_and(|status| status.success()),
            "A FIFO status read blocked or failed."
        );
        assert!(
            root.path().join("finished").is_file(),
            "The exact FIFO reader test did not run."
        );
    }

    #[test]
    fn readiness_refuses_oversized_regular_json_but_accepts_the_live_small_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let pid = std::process::id();
        let path = root.path().join(format!("{pid}.json"));
        let mut snapshot = current_readiness_snapshot();
        std::fs::write(&path, snapshot.to_string()).unwrap();
        assert_eq!(
            read_shell_state(root.path(), pid),
            Some(json!({"idle":true,"empty":true}))
        );
        snapshot["padding"] = json!("x".repeat(MAX_SHELL_STATE_BYTES as usize));
        std::fs::write(&path, snapshot.to_string()).unwrap();
        assert!(read_shell_state(root.path(), pid).is_none());
    }

    #[test]
    fn simultaneous_launches_never_share_an_env_temp_file() {
        let dir = std::env::temp_dir().join(format!("mrsm-webterm-env-{}", std::process::id()));
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: None,
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
        let mut paths: Vec<PathBuf> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        paths.sort();
        paths.dedup();
        // One file per launch (each window reads and removes its own), every temporary renamed
        // away.
        assert_eq!(paths.len(), 16);
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names.len(), 16, "{names:?}");
        assert!(
            names
                .iter()
                .all(|n| n.starts_with("webterm.") && n.ends_with(".env"))
        );
        // The shared file of earlier releases is removed by the next launch.
        std::fs::write(dir.join("webterm.env"), "OLD=1\n").unwrap();
        write_env_file(&tmux, &shell).unwrap();
        assert!(!dir.join("webterm.env").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn terminals_get_their_own_tmux_server_and_config() {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::ServerConfig::for_root("sql:main", root.path());
        // A dedicated socket under the root, in a private directory.
        let socket = tmux_socket_path(&cfg).unwrap();
        assert_eq!(socket, cfg.webterm_resources_path.join("tmux.sock"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(socket.parent().unwrap())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        // Meerschaum's configuration (the window list on top), private, rewritten when it
        // changed.
        let conf = tmux_config_path(&cfg).unwrap();
        assert_eq!(std::fs::read_to_string(&conf).unwrap(), TMUX_CONF);
        assert!(TMUX_CONF.contains("set -g status on"));
        assert!(TMUX_CONF.contains("set -g status-position top"));
        // What a running server gets again is what the file sets.
        let lines: Vec<&str> = TMUX_CONF.lines().collect();
        for (name, value) in TMUX_WINDOW_LIST {
            let plain = format!("set -g {name} {value}");
            let quoted = format!("set -g {name} \"{value}\"");
            assert!(
                lines.iter().any(|line| *line == plain || *line == quoted),
                "{name} {value:?} is not in TMUX_CONF"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&conf).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::write(&conf, "set -g status on\n").unwrap();
        assert_eq!(
            std::fs::read_to_string(tmux_config_path(&cfg).unwrap()).unwrap(),
            TMUX_CONF
        );
        // `api:webterm:tmux:config` names another file, which must exist.
        let custom = root.path().join("mine.conf");
        cfg.webterm_tmux_config = Some(custom.clone());
        assert!(tmux_config_path(&cfg).is_err());
        std::fs::write(&custom, "set -g status on\n").unwrap();
        assert_eq!(tmux_config_path(&cfg).unwrap(), custom);
        // A root too long for a socket path gets a short one in the temporary directory.
        cfg.webterm_resources_path = root.path().join("x".repeat(120));
        let short = tmux_socket_path(&cfg).unwrap();
        assert!(short.as_os_str().len() <= MAX_SOCKET_PATH + 64, "{short:?}");
        assert!(short.starts_with(std::env::temp_dir()));
        assert_eq!(short, tmux_socket_path(&cfg).unwrap());
    }

    #[test]
    fn env_values_are_quoted_for_sh() {
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
        assert!(valid_env_name("MRSM_SQL_MAIN"));
        assert!(!valid_env_name("1X"));
        assert!(!valid_env_name("A-B"));
    }
}
