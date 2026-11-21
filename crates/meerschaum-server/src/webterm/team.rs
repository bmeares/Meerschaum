//! Team terminals (5.1): socket derivation, active team server tracking, and detached-session
//! sweeper for [`crate::team_execution`].
//!
//! Multiplexer lifecycle (atomic `0600` env files, `env -i` wrappers, lockdown verification,
//! session launch, and `new-window`) is unified in `TmuxMux` under `TmuxProfile::Team`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use super::{
    MAX_SOCKET_PATH, TermMode, TmuxMux, TmuxProfile, TmuxSettings, create_private_dir,
    end_tmux_session, restricted_tmux, tmux_output, tmux_server_running,
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

/// The environment of every tmux command on a team's server (or an isolated `clear_env` command):
/// nothing of the server's but what tmux needs to find programs and read UTF-8 (the server's
/// global environment is the first client's).
pub(super) fn minimal_env() -> Vec<(String, String)> {
    ["PATH", "LANG", "LC_ALL", "LC_CTYPE"]
        .iter()
        .filter_map(|name| std::env::var(name).ok().map(|v| (name.to_string(), v)))
        .collect()
}

/// Record an active team session so the sweeper ends it if the user loses team access.
pub(super) fn record_team_session(
    socket: &Path,
    team: &str,
    settings: &TmuxSettings,
    session: &str,
    term_name: &str,
) {
    let mut servers = servers().lock().unwrap_or_else(|e| e.into_inner());
    let server = servers
        .entry(socket.to_path_buf())
        .or_insert_with(|| TeamServer {
            team: team.to_string(),
            settings: settings.clone(),
            sessions: BTreeMap::new(),
            state: None,
            sweeping: false,
        });
    server
        .sessions
        .insert(session.to_string(), term_name.to_string());
}

/// End the session `name` of every team's server (a user whose terminal is now of another mode
/// or another team never reattaches it).
pub fn end_team_sessions_of(name: &str) {
    end_team_sessions_except(name, None);
}

pub(super) fn end_team_sessions_except(name: &str, keep: Option<&Path>) {
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
        let (tmux2, team2, socket2) = (tmux.clone(), team.clone(), socket.clone());
        let live: Option<Vec<String>> = tokio::task::spawn_blocking(move || {
            if !tmux_server_running(&tmux2, &socket2) {
                return None;
            }
            let mux = TmuxMux::new(tmux2, TmuxProfile::Team(team2));
            mux.command(None).and_then(|mut command| {
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
                let removed = servers.remove(&socket).map(|s| (s.team, s.settings));
                drop(servers);
                if let Some((team, settings)) = removed {
                    stop_team_server(&team, &settings);
                }
                return;
            }
        }
    }
}

/// Stop a team's tmux server (its socket only).
fn stop_team_server(team: &str, tmux: &TmuxSettings) {
    let mux = TmuxMux::new(tmux.clone(), TmuxProfile::Team(team.to_string()));
    if let Some(socket) = &tmux.socket
        && !super::stop_owned_server(socket)
        && let Some(mut command) = mux.command(None)
    {
        command.arg("kill-server");
        let _ = tmux_output(&mut command);
    }
}

/// Stop every team's tmux server this process knows (the terminal may not use tmux now:
/// `WebtermConfig::active_tmux`).
pub fn stop_team_servers() {
    let all: Vec<(String, TmuxSettings)> = servers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .drain()
        .map(|(_, server)| (server.team, server.settings))
        .collect();
    for (team, settings) in all {
        stop_team_server(&team, &settings);
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
