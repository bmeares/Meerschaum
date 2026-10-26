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

    /// Signals are not supported on this platform.
    pub fn send_signal(_pid: i32, _sig: Sig) -> Result<(), String> {
        Err("Signals are not supported on this platform.".to_string())
    }

    /// Every process reads as gone on this platform.
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

/// What could be learned about the environment a process was started with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Environ {
    /// Its variables.
    Read(Vec<(String, String)>),
    /// Reading it was refused (another user's process) or the process is gone: psutil's
    /// `AccessDenied` / `NoSuchProcess`.
    Denied,
    /// This platform has no way to read it here, or what was read could not be parsed.
    Unknown,
}

/// The environment a process was started with: `/proc/<pid>/environ` on Linux,
/// `sysctl(KERN_PROCARGS2)` on macOS, `sysctl(KERN_PROC_ENV)` on FreeBSD (what psutil's
/// `Process.environ()` reads); [`Environ::Unknown`] elsewhere.
#[cfg(target_os = "linux")]
pub fn process_environ(pid: i32) -> Environ {
    match std::fs::read(format!("/proc/{pid}/environ")) {
        Ok(raw) => Environ::Read(parse_nul_environ(&raw)),
        Err(_) => Environ::Denied,
    }
}

/// See the Linux version.
#[cfg(target_os = "macos")]
pub fn process_environ(pid: i32) -> Environ {
    let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
    let mut argmax: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>();
    // SAFETY: `mib` names 2 integers; `argmax` is a writable `c_int` and `size` holds its size.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&mut argmax as *mut libc::c_int).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 || argmax <= 0 {
        return Environ::Unknown;
    }
    let mut buf = vec![0u8; argmax as usize];
    let mut size = buf.len();
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
    // SAFETY: `mib` names 3 integers; `buf` is writable for `size` bytes, which the kernel
    // updates to the number of bytes written.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buf.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        // EINVAL: another user's process, a zombie or a gone process (psutil's `AccessDenied`).
        return Environ::Denied;
    }
    buf.truncate(size.min(buf.len()));
    parse_procargs2(&buf).map_or(Environ::Unknown, Environ::Read)
}

/// See the Linux version.
#[cfg(target_os = "freebsd")]
pub fn process_environ(pid: i32) -> Environ {
    let mib = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_ENV, pid];
    let mut size: libc::size_t = 0;
    // SAFETY: `mib` names 4 integers; a null buffer asks only for the size, written to `size`.
    let rc = unsafe {
        libc::sysctl(
            mib.as_ptr(),
            4,
            std::ptr::null_mut(),
            &mut size,
            std::ptr::null(),
            0,
        )
    };
    if rc != 0 {
        return Environ::Denied;
    }
    // Room for an environment which grew between the two calls.
    let mut buf = vec![0u8; size + 4096];
    let mut size = buf.len();
    // SAFETY: `buf` is writable for `size` bytes, which the kernel updates to the bytes written.
    let rc = unsafe {
        libc::sysctl(
            mib.as_ptr(),
            4,
            buf.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null(),
            0,
        )
    };
    if rc != 0 {
        return Environ::Denied;
    }
    buf.truncate(size.min(buf.len()));
    Environ::Read(parse_nul_environ(&buf))
}

/// See the Linux version.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
pub fn process_environ(_pid: i32) -> Environ {
    Environ::Unknown
}

/// `KEY=value` strings separated by NUL bytes (`/proc/<pid>/environ`, FreeBSD's
/// `KERN_PROC_ENV`); strings without `=` are skipped.
pub fn parse_nul_environ(raw: &[u8]) -> Vec<(String, String)> {
    raw.split(|b| *b == 0)
        .filter_map(|kv| {
            let kv = String::from_utf8_lossy(kv);
            let (k, v) = kv.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

/// The environment in macOS's `KERN_PROCARGS2` buffer: a native-endian `int` argc, the executable
/// path, NUL padding, `argc` NUL-terminated arguments, then the NUL-terminated environment up to
/// an empty string (as psutil reads it). `None` when the buffer is shorter than its own argc
/// says (truncated or not this layout).
pub fn parse_procargs2(buf: &[u8]) -> Option<Vec<(String, String)>> {
    let argc = i32::from_ne_bytes(buf.get(..4)?.try_into().ok()?);
    if argc < 0 {
        return None;
    }
    let mut rest = &buf[4..];
    // The executable path, then its padding.
    let end = rest.iter().position(|b| *b == 0)?;
    rest = &rest[end..];
    let start = rest.iter().position(|b| *b != 0)?;
    rest = &rest[start..];
    for _ in 0..argc {
        let end = rest.iter().position(|b| *b == 0)?;
        rest = &rest[end + 1..];
    }
    let mut env = Vec::new();
    for item in rest.split(|b| *b == 0) {
        if item.is_empty() {
            break;
        }
        let kv = String::from_utf8_lossy(item);
        if let Some((k, v)) = kv.split_once('=') {
            env.push((k.to_string(), v.to_string()));
        }
    }
    Some(env)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn procargs2(argc: i32, body: &[u8]) -> Vec<u8> {
        let mut buf = argc.to_ne_bytes().to_vec();
        buf.extend_from_slice(body);
        buf
    }

    #[test]
    fn nul_separated_environments() {
        assert_eq!(
            parse_nul_environ(b"HOME=/home/u\0NOEQ\0MRSM_ROOT_DIR=/a b/r=1\0"),
            pairs(&[("HOME", "/home/u"), ("MRSM_ROOT_DIR", "/a b/r=1")])
        );
        assert_eq!(parse_nul_environ(b""), pairs(&[]));
    }

    #[test]
    fn macos_procargs2_environments() {
        let body = b"/usr/bin/python3\0\0\0\0python3\0-c\0daemon_id='x'\0\
            HOME=/Users/u\0NOEQ\0MRSM_ROOT_DIR=/a b/root\0\0ptr_munge=\0";
        assert_eq!(
            parse_procargs2(&procargs2(3, body)),
            Some(pairs(&[
                ("HOME", "/Users/u"),
                ("MRSM_ROOT_DIR", "/a b/root")
            ]))
        );
        // No environment at all.
        assert_eq!(
            parse_procargs2(&procargs2(1, b"/bin/sh\0\0sh\0")),
            Some(Vec::new())
        );
        // An argument containing spaces is not split.
        assert_eq!(
            parse_procargs2(&procargs2(1, b"/bin/sh\0sh -c x\0A=1\0")),
            Some(pairs(&[("A", "1")]))
        );
        // Fewer arguments than argc says, a negative argc, no padding end, too short.
        assert_eq!(parse_procargs2(&procargs2(5, b"/bin/sh\0\0sh\0")), None);
        assert_eq!(parse_procargs2(&procargs2(-1, b"/bin/sh\0\0sh\0")), None);
        assert_eq!(parse_procargs2(&procargs2(1, b"/bin/sh\0\0\0")), None);
        assert_eq!(parse_procargs2(b"\x01\0"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn this_process_environment_is_readable() {
        let Environ::Read(env) = process_environ(std::process::id() as i32) else {
            panic!("unreadable");
        };
        assert!(env.iter().any(|(k, _)| k == "PATH"));
        assert_eq!(process_environ(i32::MAX), Environ::Denied);
    }
}
