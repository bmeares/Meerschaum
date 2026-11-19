//! PTY process spawning and shell-state file tracking.

use super::*;

/// A running PTY: output arrives on `output`, input goes through `input`.
pub(super) struct Pty {
    pub(super) process_id: Option<u32>,
    pub(super) master: Box<dyn MasterPty + Send>,
    pub(super) input: std::sync::mpsc::SyncSender<Vec<u8>>,
    pub(super) output: mpsc::Receiver<Vec<u8>>,
    pub(super) killer: KillOnDrop,
}

/// Ends the PTY's process when dropped: the thread that reaps the process signals it.
pub(super) struct KillOnDrop(std::sync::mpsc::Sender<()>);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

pub(super) fn pty_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

pub(super) fn spawn_pty(
    builder: CommandBuilder,
    state_dir: Option<PathBuf>,
) -> std::io::Result<Pty> {
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
pub(super) fn shell_state(
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

pub(super) const MAX_SHELL_STATE_BYTES: u64 = 1024 * 1024;

/// Hold the selected directory while opening its numeric leaf: a confined shell may replace
/// its own state entries, but status reads must never follow links or wait on a FIFO/device.
pub(super) fn shell_state_file(dir: &Path, pid: u32) -> Option<std::fs::File> {
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

pub(super) fn read_shell_state(dir: &Path, pid: u32) -> Option<Value> {
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
