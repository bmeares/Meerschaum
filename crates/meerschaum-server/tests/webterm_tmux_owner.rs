//! The web terminal's tmux server outlives no API for good. A server records itself as the owner
//! of the tmux server it starts (`<socket>.owner`); an API that is killed (SIGKILL, the OOM
//! killer) never stops it, so the next API on the root adopts a server whose recorded owner is
//! gone and stops it with itself. A server whose owner still runs (another API on the same root)
//! or that has no record (started by an earlier release) is left alone.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use meerschaum_server::webterm::{self, SessionLookup, ShellCommand, WebtermConfig};

struct NoSessions;

#[async_trait]
impl SessionLookup for NoSessions {
    async fn username(&self, _: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
}

fn has_tmux() -> bool {
    std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .is_ok_and(|o| o.status.success())
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
        .prefix("mo")
        .tempdir_in(base)
        .unwrap()
}

fn tmux(socket: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new("tmux")
        .arg("-S")
        .arg(socket)
        .args(args)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .unwrap()
}

/// Kill a test's tmux server by its socket (never the default one) even if an assertion fails.
struct KillOnDrop(PathBuf);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = tmux(&self.0, &["kill-server"]);
    }
}

/// A tmux server on the root's terminal socket, as an API leaves it behind.
fn leave_server(socket: &Path) {
    let started = tmux(
        socket,
        &["-f", "/dev/null", "new-session", "-d", "-s", "left", "sh"],
    );
    assert!(started.status.success(), "{started:?}");
}

/// A process id that no longer runs.
fn dead_pid() -> u32 {
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

fn webterm_config(state: &meerschaum_server::AppState) -> WebtermConfig {
    WebtermConfig::new(
        state,
        Arc::new(NoSessions),
        Arc::new(|id| format!("<html>{id}</html>")),
        ShellCommand::new("sh"),
    )
}

async fn wait_until_stopped(socket: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while tmux(socket, &["list-sessions"]).status.success() {
        assert!(
            std::time::Instant::now() < deadline,
            "the terminal's tmux server outlived the API"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_tmux_server_left_by_a_killed_api_is_stopped_by_the_next_one() {
    if !has_tmux() {
        eprintln!("SKIP a_tmux_server_left_by_a_killed_api_is_stopped_by_the_next_one: no tmux");
        return;
    }
    let root = short_tmp();
    let (state, _) = common::state_with(common::config(root.path()));
    let socket = webterm::tmux_socket_path(&state.config).unwrap();
    let _kill = KillOnDrop(socket.clone());
    let owner = webterm::tmux_owner_path(&socket);
    leave_server(&socket);
    std::fs::write(&owner, dead_pid().to_string()).unwrap();

    // The next API on the root: no terminal has to open for it to take the server over.
    let cfg = webterm_config(&state);
    assert!(cfg.uses_tmux());
    assert!(
        webterm::owned_server_sockets().contains(&socket),
        "the server of a killed API was not adopted"
    );
    assert_eq!(
        std::fs::read_to_string(&owner).unwrap().trim(),
        std::process::id().to_string()
    );

    // ... and stops it with itself.
    assert!(webterm::stop_owned_server(&socket));
    wait_until_stopped(&socket).await;
    assert!(!owner.exists(), "the owner record outlived its server");
}

#[tokio::test]
async fn a_tmux_server_whose_owner_runs_is_left_alone() {
    if !has_tmux() {
        eprintln!("SKIP a_tmux_server_whose_owner_runs_is_left_alone: no tmux");
        return;
    }
    let root = short_tmp();
    let (state, _) = common::state_with(common::config(root.path()));
    let socket = webterm::tmux_socket_path(&state.config).unwrap();
    let _kill = KillOnDrop(socket.clone());
    let owner = webterm::tmux_owner_path(&socket);
    leave_server(&socket);
    // Another API on the same root, still running.
    let mut other = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .unwrap();
    std::fs::write(&owner, other.id().to_string()).unwrap();

    let cfg = webterm_config(&state);
    assert!(cfg.uses_tmux());
    assert!(!webterm::owned_server_sockets().contains(&socket));
    assert_eq!(
        std::fs::read_to_string(&owner).unwrap().trim(),
        other.id().to_string()
    );
    assert!(!webterm::stop_owned_server(&socket));
    assert!(tmux(&socket, &["list-sessions"]).status.success());
    let _ = other.kill();
    let _ = other.wait();
}
