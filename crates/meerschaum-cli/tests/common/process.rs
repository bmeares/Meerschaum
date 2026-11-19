//! Shared process, subreaper, background task, and tmux helpers for CLI integration tests.
#![allow(dead_code)]

use std::path::Path;
use std::process::Command;

/// Abort a spawned Tokio task automatically on drop.
pub struct AbortOnDrop<T = ()>(pub tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Become a Linux child subreaper so detached supervisor descendants cannot outlive the test.
#[cfg(target_os = "linux")]
pub struct Subreaper(Option<rustix::process::Pid>);

#[cfg(target_os = "linux")]
impl Subreaper {
    pub fn install() -> Self {
        let old = rustix::process::child_subreaper().unwrap();
        rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
        Self(old)
    }

    pub fn new() -> Self {
        Self::install()
    }
}

#[cfg(target_os = "linux")]
impl Drop for Subreaper {
    fn drop(&mut self) {
        let _ = rustix::process::set_child_subreaper(self.0);
    }
}

/// Run a `tmux -S <socket> <args...>` command from a clean environment.
pub fn tmux(socket: &Path, args: &[&str]) -> std::process::Output {
    Command::new("tmux")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .arg("-S")
        .arg(socket)
        .args(args)
        .output()
        .unwrap()
}

/// Whether `tmux` is installed and runnable in this test environment.
pub fn tmux_available() -> bool {
    Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}
