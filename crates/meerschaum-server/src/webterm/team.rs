//! Team terminals (5.1): the web terminal of a member of a team that runs code
//! ([`crate::team_execution`]).
//!
//! The shell is an `mrsm` shell that is itself a process of the team: the host of the actions
//! builds it ([`crate::backends::ActionsBackend::team_shell`]) with the team's configuration, the
//! environment allowlist and the team's sandbox spec in `require` mode, and the shell applies
//! the sandbox before it starts (`meerschaum-cli`'s `team_sandbox`), so `!command`, `sh`, `os`,
//! `python` and editors work: everything they start inherits the sandbox.
//!
//! **The multiplexer.** Each team gets a tmux server of its own ([`team_tmux`]: never shared with
//! another team, the administrators' or the restricted terminals'), locked down like the
//! restricted terminals' (no prefix, every key table emptied and verified, `default-command
//! "exit 0"`, no automatic renames, no status commands) and started from an empty environment.
//! Every window Meerschaum opens on it runs the team's shell through `env -i`, from an
//! environment file that holds only the team process's variables. The tmux server itself is
//! outside the sandbox; the team's processes cannot reach it: its socket is outside the
//! sandbox's rules, and team execution requires Landlock's pathname Unix socket rules (ABI 9),
//! which refuse a sandboxed process a connection to it, and its signal scope, which refuses it a
//! signal to the tmux server. So a new window, a respawned pane, tmux's command prompt and a
//! crashed shell all run either the sandboxed team shell or nothing.
//!
//! **Ending.** An open team terminal is authorized again like any other; any revocation (the
//! team's switch turned off, the team deleted, the user removed from it, the sandbox no longer
//! enforced) closes it and ends the user's session. A sweeper per team server ends the
//! sessions whose users may no longer open that team's terminal (detached ones included) and
//! stops the server when none is left.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use portable_pty::CommandBuilder;

use super::{
    Launched, MAX_SESSION_WINDOWS, MAX_SOCKET_PATH, ShellCommand, TMUX_WINDOW_NAME, TermMode,
    TmuxSettings, apply_restricted_conf, claim_tmux_server, create_private_dir, end_tmux_session,
    remove_stale_env_files, restricted_tmux, sh_quote, spawn_pty, tmux_output, tmux_server_running,
    tmux_session_name, valid_env_name, write_private_file, write_restricted_conf,
};
use crate::AppState;

/// A team's tmux server known to this process: its team, its sessions (`session -> user`) and
/// the state whose users the sweeper checks.
struct TeamServer {
    team: String,
    settings: TmuxSettings,
    sessions: BTreeMap<String, String>,
    state: Option<AppState>,
    sweeping: bool,
}

fn servers() -> &'static Mutex<HashMap<PathBuf, TeamServer>> {
    static SERVERS: std::sync::OnceLock<Mutex<HashMap<PathBuf, TeamServer>>> =
        std::sync::OnceLock::new();
    SERVERS.get_or_init(Default::default)
}

/// The tmux settings of `team`'s terminals next to `base` (the administrators'): a socket of its
/// own (`<stem>-t-<hash of the team>.sock` next to theirs, or in a private directory of the
/// temporary directory when that path is too long for a socket) and the locked-down
/// configuration of the restricted terminals. `None` without a dedicated socket.
pub fn team_tmux(base: &TmuxSettings, team: &str) -> Option<TmuxSettings> {
    let socket = team_socket_path(base.socket.as_ref()?, team)?;
    Some(TmuxSettings {
        socket: Some(socket),
        ..restricted_tmux(base)?
    })
}

/// Exact socket used by the Team multiplexer for this base socket and stable Team name or ID.
pub fn team_socket_path(socket: &Path, team: &str) -> Option<PathBuf> {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(team.trim().to_lowercase().as_bytes());
    let hash: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let stem = socket
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "tmux".into());
    let mut path = socket.with_file_name(format!("{stem}-t-{hash}.sock"));
    if path.as_os_str().len() > MAX_SOCKET_PATH {
        let user = std::env::var("USER")
            .unwrap_or_else(|_| "user".into())
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .collect::<String>();
        let dir = std::env::temp_dir().join(format!("mrsm-webterm-{user}"));
        create_private_dir(&dir).ok()?;
        let root: String = sha2::Sha256::digest(socket.to_string_lossy().as_bytes())
            .iter()
            .take(4)
            .map(|b| format!("{b:02x}"))
            .collect();
        path = dir.join(format!("{root}-t-{hash}.sock"));
    }
    Some(path)
}

/// The environment of every tmux command on a team's server: nothing of the server's but what
/// tmux needs to find programs and read UTF-8 (the server's global environment is the first
/// client's).
pub(super) fn minimal_env() -> Vec<(String, String)> {
    ["PATH", "LANG", "LC_ALL", "LC_CTYPE"]
        .iter()
        .filter_map(|name| std::env::var(name).ok().map(|v| (name.to_string(), v)))
        .collect()
}

/// A tmux command on a team's server (its socket only), from [`minimal_env`].
fn team_tmux_command(tmux: &TmuxSettings) -> Option<std::process::Command> {
    let socket = tmux.socket.as_ref()?;
    let mut command = std::process::Command::new(&tmux.binary);
    command
        .arg("-S")
        .arg(socket)
        .env_clear()
        .envs(minimal_env());
    Some(command)
}

/// `env`, by its absolute path (a team window starts from an empty environment, without `PATH`).
fn env_program() -> &'static str {
    ["/usr/bin/env", "/bin/env"]
        .into_iter()
        .find(|p| Path::new(p).is_file())
        .unwrap_or("/usr/bin/env")
}

/// The argv a team window runs: `env -i /bin/sh` reads the environment file (the team process's
/// variables only, `0600`, removed once read) and replaces itself with the team's shell, which
/// applies the sandbox before anything else. A file that cannot be read ends the window there.
fn team_shell_argv(tmux: &TmuxSettings, shell: &ShellCommand) -> std::io::Result<Vec<String>> {
    let mut body = String::new();
    for (key, value) in &shell.env {
        if valid_env_name(key) && key != "TMUX" && key != "TMUX_PANE" {
            body.push_str(&format!("{key}={}\n", sh_quote(value)));
        }
    }
    create_private_dir(&tmux.env_dir)?;
    remove_stale_env_files(&tmux.env_dir);
    static LAUNCHES: AtomicUsize = AtomicUsize::new(0);
    let stem = format!(
        "webterm.team.{}.{}",
        std::process::id(),
        LAUNCHES.fetch_add(1, Ordering::Relaxed)
    );
    let path = tmux.env_dir.join(format!("{stem}.env"));
    let temporary = tmux.env_dir.join(format!("{stem}.env.tmp"));
    write_private_file(&temporary, body.as_bytes())?;
    std::fs::rename(&temporary, &path)?;
    let mut argv = vec![
        env_program().to_string(),
        "-i".into(),
        "/bin/sh".into(),
        "-c".into(),
        "set -a; . \"$0\"; set +a; command -p rm -f -- \"$0\"; exec \"$@\"".into(),
        path.display().to_string(),
        shell.program.clone(),
    ];
    argv.extend(shell.args.iter().cloned());
    Ok(argv)
}

fn team_has_session(tmux: &TmuxSettings, session: &str) -> bool {
    let Some(mut command) = team_tmux_command(tmux) else {
        return false;
    };
    command.args(["has-session", "-t", &format!("={session}")]);
    tmux_output(&mut command).is_some_and(|output| output.status.success())
}

/// End the session `name` of every team's server (a user whose terminal is now of another mode
/// or another team never reattaches it).
pub fn end_team_sessions_of(name: &str) {
    end_team_sessions_except(name, None);
}

fn end_team_sessions_except(name: &str, keep: Option<&Path>) {
    let targets: Vec<TmuxSettings> = {
        let mut servers = servers().lock().unwrap_or_else(|e| e.into_inner());
        servers
            .iter_mut()
            .filter(|(socket, _)| Some(socket.as_path()) != keep)
            .filter_map(|(_, server)| {
                server
                    .sessions
                    .remove(name)
                    .map(|_| server.settings.clone())
            })
            .collect()
    };
    for tmux in targets {
        end_tmux_session(&tmux, name);
    }
}

/// Start a team terminal for `term_name` of `team` running `shell` (complete: it starts from an
/// empty environment): on the team's tmux server when the terminal uses tmux, else the shell
/// itself on the PTY.
pub(super) fn launch_team(
    config: &super::WebtermConfig,
    term_name: &str,
    team: &str,
    shell: &ShellCommand,
    check_membership: bool,
) -> std::io::Result<Launched> {
    let mut shell = shell.clone();
    for (name, value) in [("TERM", "xterm-256color"), ("COLORTERM", "truecolor")] {
        shell.env.retain(|(k, _)| k != name);
        shell.env.push((name.into(), value.into()));
    }
    // The public rule applies here too (`WebtermConfig::active_tmux`): no tmux while isolated
    // teams' sandbox does not refuse Unix sockets. A team terminal exists only where it does.
    let Some(base) = config.active_tmux() else {
        let mut builder = CommandBuilder::new(&shell.program);
        builder.args(&shell.args);
        builder.env_clear();
        for (key, value) in &shell.env {
            builder.env(key, value);
        }
        if let Some(cwd) = &shell.cwd {
            builder.cwd(cwd);
        }
        return Ok(Launched {
            pty: spawn_pty(builder, None)?,
            session: None,
            tmux: None,
            shell,
            grant: None,
            team_window: true,
        });
    };
    let tmux = team_tmux(base, team)
        .ok_or_else(|| std::io::Error::other("no tmux socket for the team's terminals"))?;
    let socket = tmux.socket.clone().expect("a team socket");
    let name = tmux_session_name(term_name, &base.session_suffix);
    static TEAM_LAUNCH: Mutex<()> = Mutex::new(());
    let _launching = TEAM_LAUNCH.lock().unwrap_or_else(|e| e.into_inner());
    // Not a session of another mode, nor of another team's server.
    end_tmux_session(base, &name);
    if let Some(restricted) = restricted_tmux(base) {
        end_tmux_session(&restricted, &name);
        config.policy.forget_session(&name);
    }
    end_team_sessions_except(&name, Some(&socket));
    write_restricted_conf(&tmux)?;
    claim_tmux_server(&tmux);
    if tmux_server_running(&tmux, &socket) && !apply_restricted_conf(&tmux) {
        return Err(std::io::Error::other(
            "the team terminal's tmux configuration could not be applied",
        ));
    }
    if !team_has_session(&tmux, &name) {
        let argv = team_shell_argv(&tmux, &shell)?;
        let mut command =
            team_tmux_command(&tmux).ok_or_else(|| std::io::Error::other("no team tmux socket"))?;
        command.arg("-u");
        if let Some(file) = &tmux.config_file {
            command.args(["-f", file]);
        }
        command
            .args(["new-session", "-d", "-s", &name, "-n", TMUX_WINDOW_NAME])
            .args(&argv)
            .env("TERM", "xterm-256color")
            .current_dir(shell.cwd.clone().unwrap_or_else(|| PathBuf::from("/")));
        if !tmux_output(&mut command).is_some_and(|output| output.status.success()) {
            return Err(std::io::Error::other(
                "tmux could not start the team terminal's session",
            ));
        }
    } else {
        // No argv for this fresh shell has been constructed or handed to tmux. An API
        // guardian's unused nonce must not survive reattachment to its existing panes.
        super::native_pending::cancel_unspawned(&shell)?;
    }
    // tmux applies all of the lockdown, or the terminal does not open.
    if !apply_restricted_conf(&tmux) {
        end_tmux_session(&tmux, &name);
        return Err(std::io::Error::other(
            "the team terminal's tmux configuration could not be applied",
        ));
    }
    if check_membership {
        let mut servers = servers().lock().unwrap_or_else(|e| e.into_inner());
        let server = servers.entry(socket.clone()).or_insert_with(|| TeamServer {
            team: team.to_string(),
            settings: tmux.clone(),
            sessions: BTreeMap::new(),
            state: None,
            sweeping: false,
        });
        server.sessions.insert(name.clone(), term_name.to_string());
    }
    let mut builder = CommandBuilder::new(&tmux.binary);
    builder.env_clear();
    for (key, value) in minimal_env() {
        builder.env(key, value);
    }
    builder.env("TERM", "xterm-256color");
    builder.arg("-S");
    builder.arg(&socket);
    builder.arg("-u");
    builder.args(["attach-session", "-t", &format!("={name}")]);
    Ok(Launched {
        pty: spawn_pty(builder, None)?,
        session: Some(name),
        tmux: Some(tmux),
        shell,
        grant: None,
        team_window: true,
    })
}

/// Open a window running the team's shell in `session` of a team's server, unless it already has
/// [`MAX_SESSION_WINDOWS`]: `Ok(false)` then.
pub(super) fn team_new_window(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
    session: &str,
) -> std::io::Result<bool> {
    static OPENING: Mutex<()> = Mutex::new(());
    let _opening = OPENING.lock().unwrap_or_else(|e| e.into_inner());
    let count = team_tmux_command(tmux).and_then(|mut command| {
        command.args([
            "list-windows",
            "-t",
            &format!("={session}"),
            "-F",
            "#{window_id}",
        ]);
        tmux_output(&mut command)
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).lines().count())
    });
    if count.is_none_or(|n| n >= MAX_SESSION_WINDOWS) {
        return Ok(false);
    }
    let shell = super::native_pending::window_shell(shell)?;
    let argv = team_shell_argv(tmux, &shell)?;
    let Some(mut command) = team_tmux_command(tmux) else {
        return Ok(false);
    };
    command
        .args([
            "new-window",
            "-t",
            &format!("={session}:"),
            "-n",
            TMUX_WINDOW_NAME,
        ])
        .args(&argv);
    Ok(tmux_output(&mut command).is_some_and(|output| output.status.success()))
}

/// Watch the team servers this process started for `state`'s terminals: every `every`, end the
/// sessions whose users may no longer open that team's terminal and stop a server with none
/// left. One task per server, started with its first terminal.
pub(super) fn start_team_sweeper(state: &AppState, every: Duration) {
    let every = every.max(Duration::from_millis(200));
    let to_start: Vec<PathBuf> = {
        let mut servers = servers().lock().unwrap_or_else(|e| e.into_inner());
        servers
            .iter_mut()
            .filter(|(_, server)| !server.sweeping)
            .map(|(socket, server)| {
                server.sweeping = true;
                server.state = Some(state.clone());
                socket.clone()
            })
            .collect()
    };
    for socket in to_start {
        tokio::spawn(sweep(socket, every));
    }
}

async fn sweep(socket: PathBuf, every: Duration) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let snapshot = {
            let servers = servers().lock().unwrap_or_else(|e| e.into_inner());
            servers.get(&socket).map(|server| {
                (
                    server.team.clone(),
                    server.settings.clone(),
                    server.sessions.clone(),
                    server.state.clone(),
                )
            })
        };
        let Some((team, tmux, sessions, Some(state))) = snapshot else {
            return;
        };
        for (session, user) in sessions {
            let keep = matches!(
                super::authorize_user(&state, &user, true).await,
                super::Access::Granted { mode: TermMode::Team(ref now), .. }
                    if meerschaum_core::access::same_team(Some(now), Some(&team))
            );
            if !keep {
                tracing::info!(
                    "Ending the team terminal of '{user}' (team '{team}'): it may no longer be \
                     opened."
                );
                let (tmux, ending) = (tmux.clone(), session.clone());
                let _ = tokio::task::spawn_blocking(move || end_tmux_session(&tmux, &ending)).await;
                let mut servers = servers().lock().unwrap_or_else(|e| e.into_inner());
                if let Some(server) = servers.get_mut(&socket) {
                    server.sessions.remove(&session);
                }
            }
        }
        // Sessions whose shells exited are gone from tmux; a server with none left stops.
        let (tmux2, socket2) = (tmux.clone(), socket.clone());
        let live: Option<Vec<String>> = tokio::task::spawn_blocking(move || {
            if !tmux_server_running(&tmux2, &socket2) {
                return None;
            }
            team_tmux_command(&tmux2).and_then(|mut command| {
                command.args(["list-sessions", "-F", "#{session_name}"]);
                tmux_output(&mut command).map(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .lines()
                        .map(str::to_string)
                        .collect()
                })
            })
        })
        .await
        .ok()
        .flatten();
        let mut servers = servers().lock().unwrap_or_else(|e| e.into_inner());
        let Some(server) = servers.get_mut(&socket) else {
            return;
        };
        match live {
            Some(live) if !live.is_empty() => {
                server.sessions.retain(|name, _| live.contains(name));
                // A session this process does not know (another API's, or a stale one) has no
                // user to check: it ends.
                for name in live.iter().filter(|n| !server.sessions.contains_key(*n)) {
                    end_tmux_session(&server.settings, name);
                }
            }
            _ => {
                let settings = servers.remove(&socket).map(|s| s.settings);
                drop(servers);
                if let Some(settings) = settings {
                    stop_team_server(&settings);
                }
                return;
            }
        }
    }
}

/// Stop a team's tmux server (its socket only).
fn stop_team_server(tmux: &TmuxSettings) {
    if let Some(socket) = &tmux.socket
        && !super::stop_owned_server(socket)
        && let Some(mut command) = team_tmux_command(tmux)
    {
        command.arg("kill-server");
        let _ = tmux_output(&mut command);
    }
}

/// Stop every team's tmux server this process knows (the terminal may not use tmux now:
/// `WebtermConfig::active_tmux`).
pub fn stop_team_servers() {
    let all: Vec<TmuxSettings> = servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .drain()
        .map(|(_, server)| server.settings)
        .collect();
    for settings in all {
        stop_team_server(&settings);
    }
}

/// The team servers this process knows (for tests): `(socket, team, sessions)`.
pub fn team_servers() -> Vec<(PathBuf, String, Vec<String>)> {
    servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|(socket, server)| {
            (
                socket.clone(),
                server.team.clone(),
                server.sessions.keys().cloned().collect(),
            )
        })
        .collect()
}
