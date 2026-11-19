//! Whether a job waits for input: the supervisor's side of the `input.stdin.block` marker.
//!
//! v4 created the marker from inside the job: the daemon's `sys.stdin` was a `StdinFile` which
//! touched `input.stdin.block` while a read found nothing to return. v5 jobs read the FIFO
//! directly (a Rust `mrsm`, its Python workers, a shell, any child), and a team job cannot write
//! in its own directory, so the process that started the job (the supervisor, or the systemd
//! wrapper) watches the job's process tree from outside and keeps the marker in step:
//!
//! - **Linux**: a thread of any process in the tree is blocked in `read`, `readv`, `pread64`,
//!   `preadv(2)` on the FIFO, or in `poll` / `ppoll` / `select` / `pselect6` / `epoll_wait` /
//!   `epoll_pwait(2)` with the FIFO among the descriptors it waits to read
//!   (`/proc/<pid>/task/<tid>/syscall`, the pointers decoded through `/proc/<pid>/mem`, an epoll
//!   set through `/proc/<pid>/fdinfo`). The supervisor is the tree's ancestor, so Yama's
//!   `ptrace_scope=1` allows the reads, and a Landlock domain does not restrict an outside
//!   observer. inotify on the FIFO reports a consumed answer at once (`IN_ACCESS`), so the
//!   marker goes away as soon as the job has read its input. Between events the tree is
//!   scanned after the job's output (a prompt usually prints its question first), backing off
//!   from [`MIN_INTERVAL`] to [`MAX_INTERVAL`] while nothing happens: a job which never reads
//!   its input costs one scan of its threads' `stat` files every [`MAX_INTERVAL`].
//!   Descendants which leave the tree (a double fork re-parents them to init) are not watched.
//!   On architectures other than x86-64 and aarch64 the syscall numbers are not decoded and
//!   nothing is detected.
//! - **Other Unix systems** (macOS): there is no way to see another process's blocked system
//!   call, so Meerschaum's own prompts announce themselves ([`announce_wait`], used by the Rust
//!   `prompt()` and Python's `meerschaum.utils.prompt.prompt()` inside a job). A plain `input()`
//!   or a shell `read` is not detected there; input written to the job still reaches it.
//! - **Windows** has no background jobs.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// The shortest time between two scans of a job's process tree.
pub const MIN_INTERVAL: Duration = Duration::from_millis(200);

/// The longest time between two scans while nothing happens.
pub const MAX_INTERVAL: Duration = Duration::from_secs(2);

/// A running watch (see the module documentation). Dropping it stops the watch (and removes
/// the marker).
pub struct StdinWatch {
    #[cfg(target_os = "linux")]
    inner: Option<linux::Watch>,
}

impl StdinWatch {
    /// Watch the process tree rooted at `root_pid` for reads of `fifo`, keeping `marker`
    /// present exactly while one waits. A no-op where waiting cannot be observed.
    pub fn spawn(fifo: &Path, marker: &Path, root_pid: i32) -> StdinWatch {
        #[cfg(target_os = "linux")]
        {
            StdinWatch {
                inner: linux::Watch::spawn(fifo, marker, root_pid),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (fifo, marker, root_pid);
            StdinWatch {}
        }
    }

    /// A handle the output readers call after the job wrote something (a prompt usually
    /// prints its question before it reads).
    pub fn nudger(&self) -> Nudger {
        #[cfg(target_os = "linux")]
        {
            Nudger {
                inner: self.inner.as_ref().map(linux::Watch::nudger),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Nudger {}
        }
    }

    /// The job ended: stop watching and remove the marker now, without waiting for the
    /// watching thread (dropping the watch joins it).
    pub fn halt(&self) {
        #[cfg(target_os = "linux")]
        if let Some(w) = &self.inner {
            w.halt();
        }
    }
}

/// Wakes a [`StdinWatch`] after output (cheap: one non-blocking write while none is pending).
#[derive(Clone)]
pub struct Nudger {
    #[cfg(target_os = "linux")]
    inner: Option<linux::Nudger>,
}

impl Nudger {
    /// The job wrote output.
    pub fn nudge(&self) {
        #[cfg(target_os = "linux")]
        if let Some(n) = &self.inner {
            n.nudge();
        }
    }
}

/// Whether a thread of the process tree rooted at `root_pid` is blocked reading `fifo`
/// (`None`: this platform cannot tell).
pub fn tree_waits_on(root_pid: i32, fifo: &Path) -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        linux::tree_waits_on(root_pid, fifo)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (root_pid, fifo);
        None
    }
}

/// Removes the marker it created when dropped (see [`announce_wait`]).
pub struct WaitAnnouncement {
    marker: Option<PathBuf>,
}

impl Drop for WaitAnnouncement {
    fn drop(&mut self) {
        if let Some(marker) = &self.marker {
            let _ = std::fs::remove_file(marker);
        }
    }
}

/// Before one of Meerschaum's own prompts reads a job's input: on systems where the supervisor
/// cannot see the wait (not Linux), create the job's `input.stdin.block` until the returned
/// guard is dropped. Does nothing outside a job (`MRSM_DAEMON_ID` unset), on Linux (the
/// supervisor watches), or when the marker cannot be written.
pub fn announce_wait() -> WaitAnnouncement {
    WaitAnnouncement {
        marker: announce_marker(),
    }
}

#[cfg(target_os = "linux")]
fn announce_marker() -> Option<PathBuf> {
    None
}

#[cfg(not(target_os = "linux"))]
fn announce_marker() -> Option<PathBuf> {
    let sc = meerschaum_core::static_config::static_config();
    let id = std::env::var(&sc.environment.daemon_id).ok()?;
    if id.is_empty() {
        return None;
    }
    let ctx = crate::daemon::JobsContext::from_env().ok()?;
    let paths = crate::config::DaemonPaths::new(&ctx.paths, &id);
    if !paths.dir.is_dir() {
        return None;
    }
    std::fs::File::create(&paths.stdin_block).ok()?;
    Some(paths.stdin_block)
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::HashSet;
    use std::fs;
    use std::os::fd::{AsFd, OwnedFd};
    use std::os::unix::fs::{FileExt, MetadataExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::Instant;

    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use nix::sys::inotify::{AddWatchFlags, InitFlags, Inotify};

    use super::{MAX_INTERVAL, MIN_INTERVAL};

    /// The wake-up pipe and its "a byte is pending" flag.
    #[derive(Clone)]
    pub struct Nudger {
        write: Arc<OwnedFd>,
        pending: Arc<AtomicBool>,
    }

    impl Nudger {
        pub fn nudge(&self) {
            if !self.pending.swap(true, Ordering::AcqRel) {
                let _ = nix::unistd::write(self.write.as_fd(), b"x");
            }
        }
    }

    pub struct Watch {
        stop: Arc<AtomicBool>,
        nudger: Nudger,
        handle: Option<JoinHandle<()>>,
        marker: PathBuf,
    }

    impl Watch {
        pub fn spawn(fifo: &Path, marker: &Path, root_pid: i32) -> Option<Watch> {
            if !cfg!(any(target_arch = "x86_64", target_arch = "aarch64")) {
                return None;
            }
            let (read, write) =
                nix::unistd::pipe2(nix::fcntl::OFlag::O_NONBLOCK | nix::fcntl::OFlag::O_CLOEXEC)
                    .ok()?;
            let inotify = Inotify::init(InitFlags::IN_NONBLOCK | InitFlags::IN_CLOEXEC).ok();
            if let Some(i) = &inotify {
                let _ = i.add_watch(fifo, AddWatchFlags::IN_ACCESS | AddWatchFlags::IN_MODIFY);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let nudger = Nudger {
                write: Arc::new(write),
                pending: Arc::new(AtomicBool::new(false)),
            };
            let handle = {
                let (stop, pending) = (stop.clone(), nudger.pending.clone());
                let (fifo, marker) = (fifo.to_path_buf(), marker.to_path_buf());
                std::thread::Builder::new()
                    .name("mrsm-stdin-watch".into())
                    .spawn(move || {
                        run(&fifo, &marker, root_pid, &stop, inotify, read, &pending);
                    })
                    .ok()?
            };
            Some(Watch {
                stop,
                nudger,
                handle: Some(handle),
                marker: marker.to_path_buf(),
            })
        }

        pub fn nudger(&self) -> Nudger {
            self.nudger.clone()
        }

        pub fn halt(&self) {
            self.stop.store(true, Ordering::Release);
            let _ = nix::unistd::write(self.nudger.write.as_fd(), b"x");
            let _ = fs::remove_file(&self.marker);
        }
    }

    impl Drop for Watch {
        fn drop(&mut self) {
            self.halt();
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
            let _ = fs::remove_file(&self.marker);
        }
    }

    fn set_marker(marker: &Path, waiting: bool) {
        if waiting {
            if !marker.exists() {
                let _ = fs::File::create(marker);
            }
        } else if marker.exists() {
            let _ = fs::remove_file(marker);
        }
    }

    fn run(
        fifo: &Path,
        marker: &Path,
        root_pid: i32,
        stop: &AtomicBool,
        inotify: Option<Inotify>,
        wake: OwnedFd,
        pending: &AtomicBool,
    ) {
        let mut interval = MIN_INTERVAL;
        let mut last_scan: Option<Instant> = None;
        let mut buf = [0u8; 64];
        while !stop.load(Ordering::Acquire) {
            let timeout = last_scan
                .map(|t| interval.saturating_sub(t.elapsed()))
                .unwrap_or(MIN_INTERVAL);
            let mut fds = vec![PollFd::new(wake.as_fd(), PollFlags::POLLIN)];
            if let Some(i) = &inotify {
                fds.push(PollFd::new(i.as_fd(), PollFlags::POLLIN));
            }
            let ms = u16::try_from(timeout.as_millis()).unwrap_or(u16::MAX);
            let _ = poll(&mut fds, PollTimeout::from(ms));
            if stop.load(Ordering::Acquire) {
                break;
            }
            let mut activity = false;
            if fds[0]
                .revents()
                .is_some_and(|r| r.contains(PollFlags::POLLIN))
            {
                while nix::unistd::read(&wake, &mut buf).is_ok_and(|n| n > 0) {}
                pending.store(false, Ordering::Release);
                activity = true;
            }
            if let Some(i) = &inotify
                && let Ok(events) = i.read_events()
            {
                // The job read what was written: its prompt is answered.
                if events
                    .iter()
                    .any(|e| e.mask.contains(AddWatchFlags::IN_ACCESS))
                {
                    set_marker(marker, false);
                }
                activity |= !events.is_empty();
            }
            if activity {
                interval = MIN_INTERVAL;
            }
            // Never scan more often than `MIN_INTERVAL`, however chatty the job.
            if let Some(t) = last_scan {
                let since = t.elapsed();
                if since < MIN_INTERVAL {
                    if activity {
                        std::thread::sleep(MIN_INTERVAL - since);
                    } else {
                        continue;
                    }
                }
            }
            if stop.load(Ordering::Acquire) {
                break;
            }
            let waiting = tree_waits_on(root_pid, fifo).unwrap_or(false);
            if stop.load(Ordering::Acquire) {
                break;
            }
            set_marker(marker, waiting);
            last_scan = Some(Instant::now());
            if !activity {
                interval = (interval * 2).min(MAX_INTERVAL);
            }
        }
    }

    /// `(st_dev, st_ino)` of a path (following symbolic links, so `/proc/<pid>/fd/<n>` names
    /// the open file).
    fn identity(path: &Path) -> Option<(u64, u64)> {
        fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
    }

    pub fn tree_waits_on(root_pid: i32, fifo: &Path) -> Option<bool> {
        if !cfg!(any(target_arch = "x86_64", target_arch = "aarch64")) {
            return None;
        }
        let target = identity(fifo)?;
        let mut seen = HashSet::new();
        let mut stack = vec![root_pid];
        while let Some(pid) = stack.pop() {
            if !seen.insert(pid) {
                continue;
            }
            let Ok(tasks) = fs::read_dir(format!("/proc/{pid}/task")) else {
                continue;
            };
            for task in tasks.flatten() {
                let tdir = task.path();
                if let Ok(children) = fs::read_to_string(tdir.join("children")) {
                    stack.extend(
                        children
                            .split_whitespace()
                            .filter_map(|c| c.parse::<i32>().ok()),
                    );
                }
                if task_waits_on(pid, &tdir, target) {
                    return Some(true);
                }
            }
        }
        Some(false)
    }

    /// The state letter of `/proc/.../stat` (after the parenthesized command name).
    fn state(stat: &str) -> Option<char> {
        stat.rsplit_once(')')?
            .1
            .split_whitespace()
            .next()?
            .chars()
            .next()
    }

    fn task_waits_on(pid: i32, tdir: &Path, target: (u64, u64)) -> bool {
        // Only a sleeping thread can be waiting for input.
        if fs::read_to_string(tdir.join("stat"))
            .ok()
            .and_then(|s| state(&s))
            != Some('S')
        {
            return false;
        }
        let Ok(syscall) = fs::read_to_string(tdir.join("syscall")) else {
            return false;
        };
        let mut words = syscall.split_whitespace();
        let Some(nr) = words.next().and_then(|n| n.parse::<libc::c_long>().ok()) else {
            return false;
        };
        let args: Vec<u64> = words
            .take(6)
            .filter_map(|a| u64::from_str_radix(a.trim_start_matches("0x"), 16).ok())
            .collect();
        if args.len() < 3 {
            return false;
        }
        let is_target =
            |fd: u64| identity(Path::new(&format!("/proc/{pid}/fd/{fd}"))) == Some(target);
        match Syscall::of(nr) {
            Some(Syscall::Read) => is_target(args[0]),
            Some(Syscall::Poll) => {
                poll_fds(pid, args[0], args[1]).is_some_and(|fds| fds.into_iter().any(&is_target))
            }
            Some(Syscall::Select) => {
                select_fds(pid, args[0], args[1]).is_some_and(|fds| fds.into_iter().any(&is_target))
            }
            Some(Syscall::Epoll) => {
                epoll_fds(pid, args[0]).is_some_and(|fds| fds.into_iter().any(&is_target))
            }
            None => false,
        }
    }

    enum Syscall {
        Read,
        Poll,
        Select,
        Epoll,
    }

    impl Syscall {
        fn of(nr: libc::c_long) -> Option<Syscall> {
            let read = [
                libc::SYS_read,
                libc::SYS_readv,
                libc::SYS_pread64,
                libc::SYS_preadv,
                libc::SYS_preadv2,
            ];
            if read.contains(&nr) {
                return Some(Syscall::Read);
            }
            if nr == libc::SYS_ppoll {
                return Some(Syscall::Poll);
            }
            if nr == libc::SYS_pselect6 {
                return Some(Syscall::Select);
            }
            if nr == libc::SYS_epoll_pwait || nr == libc::SYS_epoll_pwait2 {
                return Some(Syscall::Epoll);
            }
            #[cfg(target_arch = "x86_64")]
            {
                if nr == libc::SYS_poll {
                    return Some(Syscall::Poll);
                }
                if nr == libc::SYS_select {
                    return Some(Syscall::Select);
                }
                if nr == libc::SYS_epoll_wait {
                    return Some(Syscall::Epoll);
                }
            }
            None
        }
    }

    fn read_mem(pid: i32, addr: u64, len: usize) -> Option<Vec<u8>> {
        if addr == 0 || len == 0 || len > 1 << 16 {
            return None;
        }
        let mem = fs::File::open(format!("/proc/{pid}/mem")).ok()?;
        let mut buf = vec![0u8; len];
        mem.read_exact_at(&mut buf, addr).ok()?;
        Some(buf)
    }

    /// The descriptors a `poll` waits to read: `struct pollfd { int fd; short events; short
    /// revents; }`.
    fn poll_fds(pid: i32, addr: u64, nfds: u64) -> Option<Vec<u64>> {
        let n = usize::try_from(nfds).ok()?;
        let raw = read_mem(pid, addr, n.checked_mul(8)?)?;
        Some(
            raw.as_chunks::<8>()
                .0
                .iter()
                .filter_map(|c| {
                    let fd = i32::from_ne_bytes([c[0], c[1], c[2], c[3]]);
                    let events = i16::from_ne_bytes([c[4], c[5]]);
                    (fd >= 0 && events & (libc::POLLIN | libc::POLLPRI) != 0).then_some(fd as u64)
                })
                .collect(),
        )
    }

    /// The descriptors of a `select`'s read set (a bit array of `nfds` bits).
    fn select_fds(pid: i32, nfds: u64, addr: u64) -> Option<Vec<u64>> {
        let n = usize::try_from(nfds).ok()?.min(libc::FD_SETSIZE);
        let raw = read_mem(pid, addr, n.div_ceil(64) * 8)?;
        Some(
            (0..n)
                .filter(|i| raw[i / 8] & (1 << (i % 8)) != 0)
                .map(|i| i as u64)
                .collect(),
        )
    }

    /// The descriptors an epoll set waits to read (`tfd:` lines of `/proc/<pid>/fdinfo/<epfd>`).
    fn epoll_fds(pid: i32, epfd: u64) -> Option<Vec<u64>> {
        let info = fs::read_to_string(format!("/proc/{pid}/fdinfo/{epfd}")).ok()?;
        Some(
            info.lines()
                .filter_map(|line| {
                    let mut words = line.split_whitespace();
                    if words.next()? != "tfd:" {
                        return None;
                    }
                    let fd: u64 = words.next()?.parse().ok()?;
                    if words.next()? != "events:" {
                        return None;
                    }
                    let events = u32::from_str_radix(words.next()?, 16).ok()?;
                    (events & (libc::EPOLLIN as u32 | libc::EPOLLPRI as u32) != 0).then_some(fd)
                })
                .collect(),
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::unix::process::CommandExt;
        use std::time::Duration;

        /// The shell and every descendant must release the test runner's output pipes,
        /// including when an assertion unwinds before the fixture has finished.
        struct ProcessGroup(std::process::Child);

        impl Drop for ProcessGroup {
            fn drop(&mut self) {
                use nix::sys::signal::{Signal, killpg};
                use nix::unistd::Pid;
                let _ = killpg(Pid::from_raw(self.0.id() as i32), Signal::SIGKILL);
                let _ = self.0.wait();
            }
        }

        #[test]
        fn the_state_comes_after_the_command_name() {
            assert_eq!(state("12 (a) b) S 1 2"), Some('S'));
            assert_eq!(state("12 (sh) R 1"), Some('R'));
        }

        /// A shell `read`, a Python `select` + `readline` and a grandchild's `read` are seen; a
        /// sleeping job and one reading another pipe are not.
        #[test]
        fn blocked_readers_of_the_fifo_are_seen() {
            let dir = tempfile::tempdir().unwrap();
            let fifo = dir.path().join("input.stdin");
            crate::stdin::ensure_fifo(&fifo).unwrap();
            let check = |script: &str, expected: bool| {
                let f = crate::stdin::open_fifo_for_daemon(&fifo).unwrap();
                let child = ProcessGroup(
                    std::process::Command::new("/bin/sh")
                        .args(["-c", script])
                        .env_remove("TMUX")
                        .env_remove("TMUX_PANE")
                        .process_group(0)
                        .stdin(f)
                        .spawn()
                        .unwrap(),
                );
                let pid = child.0.id() as i32;
                // A reader is seen within 5 s; a non-reader is never seen in 1.5 s.
                let deadline =
                    Instant::now() + Duration::from_millis(if expected { 5000 } else { 1500 });
                let mut seen = false;
                while Instant::now() < deadline {
                    if tree_waits_on(pid, &fifo) == Some(true) {
                        seen = true;
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                drop(child);
                assert_eq!(seen, expected, "{script}");
            };
            check("read x", true);
            check("sh -c 'read x'; sleep 5", true);
            check("sleep 5", false);
            check("sleep 5 | cat", false);
            if std::process::Command::new("python3")
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .arg("-c")
                .arg("pass")
                .status()
                .is_ok_and(|s| s.success())
            {
                check(
                    "exec python3 -c 'import select, sys; select.select([sys.stdin], [], []); sys.stdin.readline()'",
                    true,
                );
                check(
                    "exec python3 -c 'import selectors, sys; s = selectors.DefaultSelector(); s.register(sys.stdin, selectors.EVENT_READ); s.select()'",
                    true,
                );
                check("exec python3 -c 'input()'", true);
            }
        }
    }
}
