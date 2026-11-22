//! Dedicated tmux server lifecycle for the web terminal.

use super::*;

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
pub(super) const MAX_SOCKET_PATH: usize = 100;

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
pub(super) fn dedicated_tmux(cfg: &crate::config::ServerConfig) -> std::io::Result<TmuxSettings> {
    Ok(TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket_path(cfg)?),
        session_suffix: cfg.webterm_tmux_session_suffix.clone(),
        env_dir: cfg.webterm_resources_path.clone(),
        config_file: Some(tmux_config_path(cfg)?.display().to_string()),
    })
}

/// The tmux servers (their sockets) this process started for its terminals.
pub(super) fn owned_servers() -> &'static std::sync::Mutex<std::collections::BTreeSet<PathBuf>> {
    static OWNED: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeSet<PathBuf>>> =
        std::sync::OnceLock::new();
    OWNED.get_or_init(Default::default)
}

/// The record next to a terminal's tmux socket naming the process (its id) that started the
/// server there and stops it with itself: `tmux.owner` next to `tmux.sock`.
pub fn tmux_owner_path(socket: &Path) -> PathBuf {
    socket.with_extension("owner")
}

impl TmuxMux {
    /// Whether a tmux server answers on this mux's socket.
    pub(super) fn server_running(&self) -> bool {
        let Some(mut command) = self.command(None) else {
            return false;
        };
        command.arg("list-sessions");
        tmux_output(&mut command).is_some_and(|output| output.status.success())
    }

    /// Before a terminal starts on this mux's socket: remember that this process starts the
    /// tmux server when none answers there yet (so [`stop_owned_servers`] stops it with the
    /// API). `true` when a server already answers there.
    pub(super) fn claim_server(&self) -> bool {
        let Some(socket) = &self.settings.socket else {
            return false;
        };
        if self.server_running() {
            self.adopt_orphaned_server();
            return true;
        }
        owned_servers()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(socket.clone());
        record_owner(socket);
        false
    }

    /// Take over the tmux server on this mux's socket when the API that started it is gone
    /// (killed, so it never stopped it): this process then stops it with itself
    /// ([`stop_owned_servers`]), and its sessions stay until then. A server whose recorded
    /// owner still runs (another API on the same root) or that has no record (started by an
    /// earlier release) is left alone. Called when the terminal is configured, so a server left
    /// behind is stopped even when no terminal opens, and again before each terminal starts.
    /// `true` when adopted.
    pub(super) fn adopt_orphaned_server(&self) -> bool {
        let Some(socket) = &self.settings.socket else {
            return false;
        };
        let Some(owner) = recorded_owner(socket) else {
            return false;
        };
        if !owner_gone(owner) || !self.server_running() {
            return false;
        }
        owned_servers()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(socket.clone());
        record_owner(socket);
        true
    }

    /// Unset the server's job context ([`job_context_vars`]) on this mux's tmux server: in its
    /// global environment and in the user's `session` (exact name), either of which a server
    /// started by an earlier release, from a job's environment or with an `update-environment`
    /// setting may hold. Windows opened afterwards, with the terminal's `["new_window"]` or
    /// with tmux's own keys, start without it. Nothing happens when no server answers or the
    /// session does not exist yet.
    pub(super) fn scrub_server_env(&self, session: Option<&str>) {
        let context = job_context_vars();
        // `=name`: exactly this session, never one whose name merely starts with it.
        let target = session.map(|name| format!("={name}"));
        let mut scopes: Vec<Vec<&str>> = vec![vec!["-g"]];
        if let Some(target) = &target {
            scopes.push(vec!["-t", target.as_str()]);
        }
        for scope in scopes {
            let Some(mut command) = self.command(None) else {
                return;
            };
            let Some(listing) = tmux_output(command.arg("show-environment").args(&scope)) else {
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
            // One tmux invocation: `set-environment <scope> -u A ; set-environment <scope> -u B`.
            let Some(mut command) = self.command(None) else {
                return;
            };
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
}

/// The process recorded as the owner of the tmux server on `socket` ([`tmux_owner_path`]).
pub(super) fn recorded_owner(socket: &Path) -> Option<u32> {
    std::fs::read_to_string(tmux_owner_path(socket))
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

/// Record this process as the owner of the tmux server on `socket`.
pub(super) fn record_owner(socket: &Path) {
    let _ = write_private_file(
        &tmux_owner_path(socket),
        std::process::id().to_string().as_bytes(),
    );
}

/// Whether the process `pid` no longer runs (never this process). Only Unix can tell (tmux
/// runs nowhere else); elsewhere a recorded owner counts as running.
pub(super) fn owner_gone(pid: u32) -> bool {
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

/// Apply Meerschaum's window list ([`TMUX_WINDOW_LIST`]) again to a tmux server that already runs
/// on the terminal's own socket (one `tmux -S <socket> set-option -g ... ; ...`): a server started
/// by an earlier release (whose configuration turned the status line off), or left behind by an
/// API that did not stop cleanly, keeps the options it started with otherwise. Only plain
/// `set-option -g`s: applying them again changes nothing. A file of the user's own
/// (`api:webterm:tmux:config`) is theirs to apply; nothing happens without a dedicated socket.
pub fn refresh_tmux_config(tmux: &TmuxSettings) -> bool {
    let Some(file) = &tmux.config_file else {
        return false;
    };
    if std::fs::read_to_string(file).ok().as_deref() != Some(TMUX_CONF) {
        return false;
    }
    let Some(mut command) = TmuxMux::new(tmux.clone(), TmuxProfile::Full).command(None) else {
        return false;
    };
    for (i, (name, value)) in TMUX_WINDOW_LIST.iter().enumerate() {
        if i > 0 {
            command.arg(";");
        }
        command.args(["set-option", "-g", name, value]);
    }
    tmux_output(&mut command).is_some_and(|output| output.status.success())
}

/// Run a tmux command within [`TMUX_TIMEOUT`] (killed when it takes longer): its output, or
/// `None` when it could not start or did not finish in time. Every tmux command a terminal runs
/// while it is open goes through here, so a hung tmux server never holds a terminal's tasks.
pub(super) fn tmux_output(command: &mut std::process::Command) -> Option<std::process::Output> {
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

pub(super) fn kill_tmux_server(socket: &Path) {
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
