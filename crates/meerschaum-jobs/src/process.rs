//! Process inspection and signals: the parts of `psutil` the daemon code uses.

/// What `psutil.Process(pid).status()` reports, reduced to what `Daemon.status` distinguishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcState {
    /// Any live state other than stopped or zombie (`running`, `sleeping`, `disk-sleep`, ...).
    Alive,
    /// `stopped` (`T`: SIGSTOP / SIGTSTP).
    Stopped,
    /// `zombie` (`Z`).
    Zombie,
    /// `NoSuchProcess`.
    Gone,
}

/// A signal the daemon code sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sig {
    /// `SIGINT` (`Daemon.quit`).
    Int,
    /// `SIGTERM` (`Daemon.kill`, `psutil.Process.terminate`).
    Term,
    /// `SIGKILL` (`psutil.Process.kill`).
    Kill,
    /// `SIGSTOP` (`psutil.Process.suspend`).
    Stop,
    /// `SIGCONT` (`psutil.Process.resume`).
    Cont,
}

impl Sig {
    /// `str(signal.Signals.X)`: `Signals.SIGINT`.
    pub fn py_str(self) -> &'static str {
        match self {
            Sig::Int => "Signals.SIGINT",
            Sig::Term => "Signals.SIGTERM",
            Sig::Kill => "Signals.SIGKILL",
            Sig::Stop => "Signals.SIGSTOP",
            Sig::Cont => "Signals.SIGCONT",
        }
    }
}

#[cfg(unix)]
mod imp {
    use super::{ProcState, Sig};
    use nix::sys::signal::{self, Signal};
    use nix::unistd::Pid;

    fn to_signal(sig: Sig) -> Signal {
        match sig {
            Sig::Int => Signal::SIGINT,
            Sig::Term => Signal::SIGTERM,
            Sig::Kill => Signal::SIGKILL,
            Sig::Stop => Signal::SIGSTOP,
            Sig::Cont => Signal::SIGCONT,
        }
    }

    /// `os.kill(pid, sig)`.
    pub fn send_signal(pid: i32, sig: Sig) -> Result<(), String> {
        signal::kill(Pid::from_raw(pid), to_signal(sig)).map_err(|e| e.to_string())
    }

    fn exists(pid: i32) -> bool {
        if pid <= 0 {
            return false;
        }
        match signal::kill(Pid::from_raw(pid), None) {
            Ok(()) => true,
            Err(nix::errno::Errno::EPERM) => true,
            Err(_) => false,
        }
    }

    /// The state of a process.
    #[cfg(target_os = "linux")]
    pub fn proc_state(pid: i32) -> ProcState {
        if pid <= 0 {
            return ProcState::Gone;
        }
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return if exists(pid) {
                ProcState::Alive
            } else {
                ProcState::Gone
            };
        };
        // `pid (comm) S ...`: `comm` may contain spaces and parentheses.
        let state = stat
            .rfind(')')
            .and_then(|i| stat[i + 1..].trim_start().chars().next());
        match state {
            Some('T') => ProcState::Stopped,
            Some('Z') => ProcState::Zombie,
            Some('X') | Some('x') if !exists(pid) => ProcState::Gone,
            _ => ProcState::Alive,
        }
    }

    /// The state of a process.
    #[cfg(not(target_os = "linux"))]
    pub fn proc_state(pid: i32) -> ProcState {
        if !exists(pid) {
            return ProcState::Gone;
        }
        let out = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output();
        let stat = out
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        match stat.chars().next() {
            None => ProcState::Gone,
            Some('T') => ProcState::Stopped,
            Some('Z') => ProcState::Zombie,
            _ => ProcState::Alive,
        }
    }

    /// The command lines of every process: `(pid, argv)`.
    #[cfg(target_os = "linux")]
    pub fn process_cmdlines() -> Vec<(i32, Vec<String>)> {
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            let Ok(raw) = std::fs::read(entry.path().join("cmdline")) else {
                continue;
            };
            let args = raw
                .split(|b| *b == 0)
                .filter(|a| !a.is_empty())
                .map(|a| String::from_utf8_lossy(a).to_string())
                .collect();
            out.push((pid, args));
        }
        out
    }

    /// The command lines of every process: `(pid, argv)` (approximated from `ps`).
    #[cfg(not(target_os = "linux"))]
    pub fn process_cmdlines() -> Vec<(i32, Vec<String>)> {
        let Ok(out) = std::process::Command::new("ps")
            .args(["-axww", "-o", "pid=,args="])
            .output()
        else {
            return Vec::new();
        };
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|line| {
                let line = line.trim_start();
                let (pid, args) = line.split_once(' ')?;
                Some((pid.parse().ok()?, vec![args.to_string()]))
            })
            .collect()
    }
}

#[cfg(not(unix))]
mod imp {
    use super::{ProcState, Sig};

    pub fn send_signal(_pid: i32, _sig: Sig) -> Result<(), String> {
        Err("Signals are not supported on this platform.".to_string())
    }

    pub fn proc_state(_pid: i32) -> ProcState {
        ProcState::Gone
    }

    pub fn process_cmdlines() -> Vec<(i32, Vec<String>)> {
        Vec::new()
    }
}

pub use imp::{proc_state, send_signal};

static CMDLINE_SCANS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many times the process table has been enumerated by [`process_cmdlines`] in this process
/// (tests assert that status reads of completed jobs enumerate nothing).
pub fn cmdline_scans() -> usize {
    CMDLINE_SCANS.load(std::sync::atomic::Ordering::SeqCst)
}

/// The command lines of every process: `(pid, argv)`. This reads `/proc/*/cmdline`: it is
/// expensive on a busy host, so job code goes through [`crate::detached`], which shares and
/// bounds it.
pub fn process_cmdlines() -> Vec<(i32, Vec<String>)> {
    CMDLINE_SCANS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    imp::process_cmdlines()
}

/// The direct children of a process (Linux: `/proc/<pid>/task/*/children`). `None` when the
/// kernel does not expose them (`CONFIG_PROC_CHILDREN` is off, or this is not Linux), which is
/// not the same as having no children.
pub fn children(pid: i32) -> Option<Vec<i32>> {
    let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).ok()?;
    let mut out = Vec::new();
    let mut readable = false;
    for task in tasks.flatten() {
        if let Ok(text) = std::fs::read_to_string(task.path().join("children")) {
            readable = true;
            out.extend(
                text.split_whitespace()
                    .filter_map(|p| p.parse::<i32>().ok()),
            );
        }
    }
    if !readable {
        return None;
    }
    out.sort_unstable();
    out.dedup();
    Some(out)
}

/// When the process started (naive UTC), from `/proc/<pid>/stat` and the boot time: with the PID
/// it identifies one process, so a reused PID is told apart. `None` when it cannot be read
/// (not Linux, or the process is gone).
#[cfg(target_os = "linux")]
pub fn process_start_time(pid: i32) -> Option<chrono::NaiveDateTime> {
    use std::sync::OnceLock;
    static BOOT: OnceLock<Option<i64>> = OnceLock::new();
    let boot = (*BOOT.get_or_init(|| {
        std::fs::read_to_string("/proc/stat")
            .ok()?
            .lines()
            .find_map(|l| l.strip_prefix("btime ")?.trim().parse::<i64>().ok())
    }))?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 22 (`starttime`, clock ticks since boot); count from the last `)` since the command
    // name may contain spaces and parentheses.
    let ticks: f64 = stat[stat.rfind(')')? + 2..]
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()?;
    // USER_HZ is 100 on every Linux ABI `/proc` exposes it for.
    let secs = boot as f64 + ticks / 100.0;
    chrono::DateTime::from_timestamp(secs.floor() as i64, ((secs.fract()) * 1e9) as u32)
        .map(|d| d.naive_utc())
}

/// See the Linux version.
#[cfg(not(target_os = "linux"))]
pub fn process_start_time(_pid: i32) -> Option<chrono::NaiveDateTime> {
    None
}

/// The command-line marker identifying a daemon's process: `daemon_id='<id>'`.
pub fn daemon_marker(daemon_id: &str) -> String {
    format!("daemon_id='{daemon_id}'")
}

/// Wait until `pid` no longer exists (or is a zombie), up to `timeout`.
pub fn wait_gone(pid: i32, timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now().checked_add(timeout);
    loop {
        if matches!(proc_state(pid), ProcState::Gone | ProcState::Zombie) {
            return true;
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
