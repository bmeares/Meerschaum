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

pub use imp::{proc_state, process_cmdlines, send_signal};

/// Port of `Daemon._find_detached_pids`: the PIDs (other than ours) whose command line contains
/// `daemon_id='<id>'`. Python daemons embed it in their `python -c` code; the Rust supervisor
/// passes it as an argument for the same reason.
pub fn find_detached_pids(daemon_id: &str) -> Vec<i32> {
    let marker = daemon_marker(daemon_id);
    let me = std::process::id() as i32;
    process_cmdlines()
        .into_iter()
        .filter(|(pid, args)| *pid != me && args.iter().any(|a| a.contains(&marker)))
        .map(|(pid, _)| pid)
        .collect()
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
