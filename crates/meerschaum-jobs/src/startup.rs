//! Per-root job startup admission (wave 18, design decision D23): a gate every job passes before
//! its worker starts, so that starting many jobs at once (`compose up`, `restart jobs`, a reboot
//! bringing every systemd unit up together) does not make them all boot, import plugins and run
//! their first lap on top of each other.
//!
//! The gate lives in the processes which *host* a worker (the detached supervisor and the
//! `systemd` unit wrapper), so a waiting job costs one idle process, not a booted interpreter:
//!
//! 1. **Jitter**: a random delay in `[0, jitter_seconds]` (bulk starts only).
//! 2. **Pressure**: wait while the 1-minute load average is above `ncpu * max_load` (`ncpu` the
//!    host's processors: the load average is host-wide, whatever CPU limit a container has) or
//!    the available memory is below `min_free_memory_percent` (bulk starts only; what is left of
//!    the cgroup's memory limit when there is one, else the host's `MemAvailable`). Platforms
//!    without the probes (everything but Linux for memory, Windows for both) treat the check as
//!    passing.
//! 3. **Slot**: take one of `max_concurrent` file-lock slots under `<root>/jobs/.startup/`
//!    (`slot-<n>.lock`, an `flock`, so it works across the unrelated processes which start jobs on
//!    one root and is freed by the kernel when its holder dies).
//!
//! The slot is held until the worker's **first lap** is done: the worker calls
//! [`signal_first_lap_done`] (the lap loops of `sync pipes --loop`, `-s` schedules and pipelines
//! do), or exits, or `slot_timeout_seconds` pass. The wait for pressure and for a slot is capped
//! by the same timeout: after that the job starts anyway (a busy host never starves a job
//! forever). Workers which never finish a lap (a plugin action with a loop of its own, a
//! server) are not left holding the slot for the whole timeout: on Linux it is also freed once
//! the worker and its children have been nearly idle (under 10% of one core) for
//! `settle_seconds` (default 15; `0` turns this off).
//!
//! How a start is classified is [`StartKind`]: an explicit `start job x` is [`StartKind::Single`]
//! (no jitter, no pressure wait; the slot is still respected), a bulk or automatic start is
//! [`StartKind::Bulk`], and `--no-stagger` is [`StartKind::None`] (no gate at all).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use meerschaum_core::value::{Map, Value};

/// The directory of the slots under `<root>/jobs/`.
pub const STARTUP_DIR: &str = ".startup";

/// Set in the worker's environment while the host holds a startup slot for it:
/// `<host pid>:<release file>`. [`signal_first_lap_done`] creates the file.
pub const RELEASE_ENV: &str = "MRSM_JOB_STARTUP_RELEASE";

/// How the launching process classifies a start (`single`, `bulk` or `none`); read by
/// `start jobs`, set by hosts which start many jobs (compose).
pub const KIND_ENV: &str = "MRSM_JOB_START_KIND";

/// Hints of explicit starts older than this are ignored.
const HINT_MAX_AGE: Duration = Duration::from_secs(120);

/// How a job's start is classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartKind {
    /// A bulk or automatic start (`compose up`, `restart jobs`, a unit restarting): jitter,
    /// pressure wait and slot.
    Bulk,
    /// An explicit start of one job: no jitter and no pressure wait, but the slot is respected.
    #[default]
    Single,
    /// `--no-stagger`: no gate at all.
    None,
}

impl StartKind {
    /// `bulk`, `single` or `none`.
    pub fn as_str(self) -> &'static str {
        match self {
            StartKind::Bulk => "bulk",
            StartKind::Single => "single",
            StartKind::None => "none",
        }
    }

    /// The kind named by `text` (case-insensitive; `no-stagger`, `skip` and `off` mean `none`).
    pub fn parse(text: &str) -> Option<StartKind> {
        match text.trim().to_ascii_lowercase().as_str() {
            "bulk" | "auto" | "automatic" => Some(StartKind::Bulk),
            "single" | "explicit" => Some(StartKind::Single),
            "none" | "no-stagger" | "skip" | "off" => Some(StartKind::None),
            _ => None,
        }
    }

    /// The kind in `$MRSM_JOB_START_KIND`, if set to a known value.
    pub fn from_env() -> Option<StartKind> {
        std::env::var(KIND_ENV)
            .ok()
            .and_then(|v| StartKind::parse(&v))
    }
}

/// `jobs.startup.*`: the configured admission limits.
#[derive(Debug, Clone, PartialEq)]
pub struct StartupConfig {
    /// `enabled`: `false` turns the gate off (every start behaves like `--no-stagger`).
    pub enabled: bool,
    /// `jitter_seconds`: the random delay before a bulk start (default 30).
    pub jitter_seconds: f64,
    /// `max_load`: wait while the 1-minute load average is above `ncpu * max_load` (default
    /// 1.5; `0` disables the check).
    pub max_load: f64,
    /// `min_free_memory_percent`: wait while less memory than this is available (default 10;
    /// `0` disables the check).
    pub min_free_memory_percent: f64,
    /// `max_concurrent`: the number of slots (`None`: `max(2, ncpu / 2)`; `0`: no slots).
    pub max_concurrent: Option<usize>,
    /// `slot_timeout_seconds`: how long a slot is held, and how long a job waits for one
    /// (default 300).
    pub slot_timeout_seconds: f64,
    /// `settle_seconds`: free the slot once the worker (and its children) has used under 10% of
    /// one core for this long (default 15; `0` disables; Linux only).
    pub settle_seconds: f64,
}

impl Default for StartupConfig {
    fn default() -> Self {
        StartupConfig {
            enabled: true,
            jitter_seconds: 30.0,
            max_load: 1.5,
            min_free_memory_percent: 10.0,
            max_concurrent: None,
            slot_timeout_seconds: 300.0,
            settle_seconds: 15.0,
        }
    }
}

impl StartupConfig {
    /// Apply the keys of a `jobs.startup` map (or a job's `startup` override) over `self`.
    /// Ill-typed or negative values keep what was there. A job override may also say
    /// `stagger: false`.
    pub fn apply(&mut self, map: &Map) {
        let number = |key: &str| {
            map.get(key)
                .filter(|v| !matches!(v, Value::Bool(_)))
                .and_then(Value::as_f64)
                .filter(|n| n.is_finite() && *n >= 0.0)
        };
        if let Some(Value::Bool(b)) = map.get("enabled") {
            self.enabled = *b;
        }
        if let Some(Value::Bool(b)) = map.get("stagger") {
            self.enabled = *b;
        }
        if let Some(n) = number("jitter_seconds") {
            self.jitter_seconds = n;
        }
        if let Some(n) = number("max_load") {
            self.max_load = n;
        }
        if let Some(n) = number("min_free_memory_percent") {
            self.min_free_memory_percent = n;
        }
        if let Some(n) = number("max_concurrent") {
            self.max_concurrent = Some(n as usize);
        }
        if let Some(n) = number("slot_timeout_seconds") {
            self.slot_timeout_seconds = n;
        }
        if let Some(n) = number("settle_seconds") {
            self.settle_seconds = n;
        }
    }

    /// The number of slots for `ncpu` processors.
    pub fn slots(&self, ncpu: usize) -> usize {
        self.max_concurrent.unwrap_or_else(|| (ncpu / 2).max(2))
    }
}

/// The logical processors available to this process.
pub fn cpu_count() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from)
}

/// What a start is subject to, resolved by the launching process (or by the unit wrapper) and
/// handed to the host of the worker.
#[derive(Debug, Clone, PartialEq)]
pub struct StartupPolicy {
    /// How the start was classified.
    pub kind: StartKind,
    /// The random delay bound (`0` for anything but a bulk start).
    pub jitter_seconds: f64,
    /// See [`StartupConfig::max_load`] (`0` for anything but a bulk start).
    pub max_load: f64,
    /// See [`StartupConfig::min_free_memory_percent`] (`0` for anything but a bulk start).
    pub min_free_memory_percent: f64,
    /// The number of slots (`0`: none).
    pub max_concurrent: usize,
    /// See [`StartupConfig::slot_timeout_seconds`].
    pub slot_timeout_seconds: f64,
    /// See [`StartupConfig::settle_seconds`].
    pub settle_seconds: f64,
    /// `<root>/jobs/.startup`.
    pub dir: PathBuf,
}

impl StartupPolicy {
    /// The policy of a start of `kind`: `config` patched with the job's own `startup` override.
    /// `None` when nothing is gated (`--no-stagger`, `enabled: false`).
    pub fn resolve(
        config: &StartupConfig,
        job_override: Option<&Map>,
        kind: StartKind,
        jobs_dir: &Path,
    ) -> Option<StartupPolicy> {
        if kind == StartKind::None {
            return None;
        }
        let mut cfg = config.clone();
        if let Some(over) = job_override {
            cfg.apply(over);
        }
        if !cfg.enabled {
            return None;
        }
        let bulk = kind == StartKind::Bulk;
        Some(StartupPolicy {
            kind,
            jitter_seconds: if bulk { cfg.jitter_seconds } else { 0.0 },
            max_load: if bulk { cfg.max_load } else { 0.0 },
            min_free_memory_percent: if bulk {
                cfg.min_free_memory_percent
            } else {
                0.0
            },
            max_concurrent: cfg.slots(cpu_count()),
            slot_timeout_seconds: cfg.slot_timeout_seconds,
            settle_seconds: cfg.settle_seconds,
            dir: jobs_dir.join(STARTUP_DIR),
        })
    }

    /// The policy as one supervisor argument.
    pub fn to_arg(&self) -> String {
        serde_json::json!({
            "kind": self.kind.as_str(),
            "jitter_seconds": self.jitter_seconds,
            "max_load": self.max_load,
            "min_free_memory_percent": self.min_free_memory_percent,
            "max_concurrent": self.max_concurrent,
            "slot_timeout_seconds": self.slot_timeout_seconds,
            "settle_seconds": self.settle_seconds,
            "dir": self.dir.to_string_lossy(),
        })
        .to_string()
    }

    /// Decode [`StartupPolicy::to_arg`].
    pub fn from_arg(text: &str) -> Result<StartupPolicy, String> {
        let json: serde_json::Value =
            serde_json::from_str(text).map_err(|e| format!("Invalid startup policy: {e}"))?;
        let number = |key: &str| json.get(key).and_then(serde_json::Value::as_f64);
        let dir = json
            .get("dir")
            .and_then(serde_json::Value::as_str)
            .ok_or("The startup policy has no directory.")?;
        Ok(StartupPolicy {
            kind: json
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .and_then(StartKind::parse)
                .unwrap_or(StartKind::Single),
            jitter_seconds: number("jitter_seconds").unwrap_or(0.0),
            max_load: number("max_load").unwrap_or(0.0),
            min_free_memory_percent: number("min_free_memory_percent").unwrap_or(0.0),
            max_concurrent: number("max_concurrent").unwrap_or(0.0) as usize,
            slot_timeout_seconds: number("slot_timeout_seconds").unwrap_or(300.0),
            settle_seconds: number("settle_seconds").unwrap_or(0.0),
            dir: PathBuf::from(dir),
        })
    }
}

// --- system probes ---------------------------------------------------------------------------

/// The 1-minute load average (`None` where the platform has none).
#[cfg(unix)]
pub fn load_average() -> Option<f64> {
    let mut loads = [0f64; 3];
    // SAFETY: `getloadavg` writes at most `nelem` doubles into the buffer, which holds three.
    let n = unsafe { libc::getloadavg(loads.as_mut_ptr(), 3) };
    (n >= 1).then_some(loads[0])
}

/// The 1-minute load average (`None` where the platform has none).
#[cfg(not(unix))]
pub fn load_average() -> Option<f64> {
    None
}

/// Where the probes read: `/proc` and the cgroup filesystem (tests point them at fake trees).
#[derive(Debug, Clone)]
pub struct SystemPaths {
    /// `/proc`.
    pub proc_dir: PathBuf,
    /// The cgroup filesystem (`/sys/fs/cgroup`).
    pub cgroup_dir: PathBuf,
}

impl Default for SystemPaths {
    fn default() -> SystemPaths {
        SystemPaths {
            proc_dir: PathBuf::from("/proc"),
            cgroup_dir: PathBuf::from("/sys/fs/cgroup"),
        }
    }
}

/// The processors of the **host**, not only the ones this process may use: the load average is
/// host-wide, so a container limited to one CPU on a 64-CPU host must not compare it with 1
/// (`/proc/cpuinfo`, else `/proc/stat`; the process's own count where neither is readable).
pub fn host_cpu_count() -> usize {
    host_cpu_count_in(&SystemPaths::default())
}

/// [`host_cpu_count`] over `paths`.
pub fn host_cpu_count_in(paths: &SystemPaths) -> usize {
    let from_cpuinfo = fs::read_to_string(paths.proc_dir.join("cpuinfo"))
        .ok()
        .map(|text| {
            text.lines()
                .filter(|l| {
                    l.strip_prefix("processor")
                        .is_some_and(|rest| rest.trim_start().starts_with(':'))
                })
                .count()
        })
        .filter(|n| *n > 0);
    let from_stat = || {
        fs::read_to_string(paths.proc_dir.join("stat"))
            .ok()
            .map(|text| {
                text.lines()
                    .filter(|l| {
                        l.strip_prefix("cpu")
                            .and_then(|rest| rest.chars().next())
                            .is_some_and(|c| c.is_ascii_digit())
                    })
                    .count()
            })
            .filter(|n| *n > 0)
    };
    from_cpuinfo
        .or_else(from_stat)
        .map_or_else(cpu_count, |n| n.max(cpu_count()))
}

/// The share of memory available to new processes, in percent: inside a cgroup with a memory
/// limit (a container, a systemd unit with `MemoryMax=`) what is left of the limit, else the
/// host's `MemAvailable` (`/proc/meminfo`). `None` where the platform does not report it.
pub fn available_memory_percent() -> Option<f64> {
    available_memory_percent_in(&SystemPaths::default())
}

/// [`available_memory_percent`] over `paths`.
pub fn available_memory_percent_in(paths: &SystemPaths) -> Option<f64> {
    cgroup_memory_available_percent(paths).or_else(|| host_memory_available_percent(paths))
}

fn host_memory_available_percent(paths: &SystemPaths) -> Option<f64> {
    let text = fs::read_to_string(paths.proc_dir.join("meminfo")).ok()?;
    let field = |name: &str| -> Option<f64> {
        text.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.trim_start_matches(':').split_whitespace().next())
            .and_then(|n| n.parse::<f64>().ok())
    };
    let total = field("MemTotal")?;
    let available = field("MemAvailable")?;
    (total > 0.0).then(|| available / total * 100.0)
}

/// What is left of the tightest memory limit on this process's cgroup path, in percent (cgroup
/// v2 `memory.max` / `memory.current`, else v1 `memory.limit_in_bytes` / `memory.usage_in_bytes`),
/// with the reclaimable file cache (`inactive_file`) not counted as used, as container runtimes
/// report it. `None` when no cgroup on the path has a limit.
fn cgroup_memory_available_percent(paths: &SystemPaths) -> Option<f64> {
    // `0::/system.slice/x.service` (v2), or `N:memory:/path` among several lines (v1). Inside a
    // container with its own cgroup namespace the path is `/`.
    let membership = fs::read_to_string(paths.proc_dir.join("self/cgroup")).unwrap_or_default();
    let mut relative = String::new();
    let mut v1 = false;
    for line in membership.lines() {
        let mut parts = line.splitn(3, ':');
        let (_, controllers, path) = (parts.next(), parts.next(), parts.next());
        match (controllers, path) {
            (Some(""), Some(path)) => relative = path.to_string(),
            (Some(c), Some(path)) if c.split(',').any(|c| c == "memory") && relative.is_empty() => {
                relative = path.to_string();
                v1 = true;
            }
            _ => {}
        }
    }
    let number = |path: PathBuf| -> Option<f64> {
        let text = fs::read_to_string(path).ok()?;
        let text = text.trim();
        // v2 says `max` for no limit; v1 says a huge number.
        text.parse::<f64>().ok().filter(|n| *n < 1e18)
    };
    let inactive_file = |dir: &Path| -> f64 {
        fs::read_to_string(dir.join("memory.stat"))
            .ok()
            .and_then(|stat| {
                stat.lines().find_map(|l| {
                    l.strip_prefix("inactive_file ")
                        .or_else(|| l.strip_prefix("total_inactive_file "))
                        .and_then(|n| n.trim().parse::<f64>().ok())
                })
            })
            .unwrap_or(0.0)
    };
    let (limit_file, usage_file, root) = if v1 {
        (
            "memory.limit_in_bytes",
            "memory.usage_in_bytes",
            paths.cgroup_dir.join("memory"),
        )
    } else {
        ("memory.max", "memory.current", paths.cgroup_dir.clone())
    };
    let mut dir = root.join(relative.trim_start_matches('/'));
    let mut tightest: Option<f64> = None;
    loop {
        if let Some(limit) = number(dir.join(limit_file))
            && limit > 0.0
            && let Some(usage) = number(dir.join(usage_file))
        {
            let used = (usage - inactive_file(&dir)).max(0.0);
            let free = ((limit - used) / limit * 100.0).clamp(0.0, 100.0);
            tightest = Some(tightest.map_or(free, |t| t.min(free)));
        }
        if dir == root || !dir.starts_with(&root) {
            break;
        }
        if !dir.pop() {
            break;
        }
    }
    tightest
}

/// The CPU time (in clock ticks, 100 per second) used so far by `pid` and its descendants (at most
/// 256 processes). `None` where the platform does not report it.
#[cfg(target_os = "linux")]
pub fn tree_cpu_ticks(pid: i32) -> Option<u64> {
    fn own(pid: i32) -> Option<u64> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // The command name (field 2) may contain spaces and parentheses: count from the last `)`.
        let rest = &stat[stat.rfind(')')? + 2..];
        let mut fields = rest.split_whitespace().skip(11);
        let utime: u64 = fields.next()?.parse().ok()?;
        let stime: u64 = fields.next()?.parse().ok()?;
        Some(utime + stime)
    }
    let mut total = own(pid)?;
    let mut queue = vec![pid];
    let mut seen = 1;
    while let Some(parent) = queue.pop() {
        for child in crate::process::children(parent).unwrap_or_default() {
            if seen >= 256 {
                return Some(total);
            }
            seen += 1;
            total += own(child).unwrap_or(0);
            queue.push(child);
        }
    }
    Some(total)
}

/// The CPU time used by `pid` and its descendants (`None` on this platform).
#[cfg(not(target_os = "linux"))]
pub fn tree_cpu_ticks(_pid: i32) -> Option<u64> {
    None
}

/// The probes the gate reads; tests replace them.
#[derive(Debug, Clone, Copy)]
pub struct Probes {
    /// The 1-minute load average.
    pub load: fn() -> Option<f64>,
    /// The available memory, in percent.
    pub memory: fn() -> Option<f64>,
    /// The processors.
    pub ncpu: fn() -> usize,
}

impl Probes {
    /// The real probes.
    pub fn system() -> Probes {
        Probes {
            load: load_average,
            memory: available_memory_percent,
            // The load average is the host's: compare it with the host's processors, not the
            // ones a container's CPU limit leaves this process.
            ncpu: host_cpu_count,
        }
    }
}

/// Why the host is too busy to start another job right now (`None`: it is not).
pub fn pressure(policy: &StartupPolicy, probes: &Probes) -> Option<String> {
    if policy.max_load > 0.0
        && let Some(load) = (probes.load)()
    {
        let limit = (probes.ncpu)() as f64 * policy.max_load;
        if load > limit {
            return Some(format!(
                "the load average is {load:.2}, above {limit:.2} ({} CPUs x {})",
                (probes.ncpu)(),
                policy.max_load
            ));
        }
    }
    if policy.min_free_memory_percent > 0.0
        && let Some(free) = (probes.memory)()
        && free < policy.min_free_memory_percent
    {
        return Some(format!(
            "only {free:.1}% of memory is available, below {}%",
            policy.min_free_memory_percent
        ));
    }
    None
}

// --- jitter ----------------------------------------------------------------------------------

/// A random number in `[0, 1)`.
fn random_unit() -> f64 {
    let mut buf = [0u8; 8];
    let _ = getrandom::fill(&mut buf);
    (u64::from_le_bytes(buf) >> 11) as f64 / (1u64 << 53) as f64
}

/// The fixed offset of `seed` (a job's name) within `[0, jitter_seconds]`, in whole
/// milliseconds: the same job always gets the same offset (FNV-1a, stable across releases).
pub fn fixed_offset(seed: &str, jitter_seconds: f64) -> Duration {
    if !(jitter_seconds.is_finite() && jitter_seconds > 0.0) {
        return Duration::ZERO;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let span_ms = (jitter_seconds * 1000.0).round() as u64;
    Duration::from_millis(hash % (span_ms + 1))
}

// --- slots -----------------------------------------------------------------------------------

/// The path of slot `index`.
pub fn slot_path(dir: &Path, index: usize) -> PathBuf {
    dir.join(format!("slot-{index}.lock"))
}

/// A held slot (the `flock` is released when it is dropped, or when its process dies).
#[derive(Debug)]
pub struct SlotGuard {
    #[cfg(unix)]
    _lock: nix::fcntl::Flock<fs::File>,
    index: usize,
}

impl SlotGuard {
    /// Which slot this is.
    pub fn index(&self) -> usize {
        self.index
    }
}

/// Take the first free slot of `0..count`. Each slot file names its holder (for people looking).
#[cfg(unix)]
pub fn try_acquire_slot(dir: &Path, count: usize, job: &str) -> Option<SlotGuard> {
    use nix::fcntl::{Flock, FlockArg};
    use std::io::Write;
    fs::create_dir_all(dir).ok()?;
    for index in 0..count {
        let Ok(file) = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(slot_path(dir, index))
        else {
            continue;
        };
        if let Ok(lock) = Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            let guard = SlotGuard { _lock: lock, index };
            let info = format!("{} {job}\n", std::process::id());
            // The lock owns the only handle; rewrite through a second one.
            if let Ok(mut f) = fs::OpenOptions::new()
                .write(true)
                .open(slot_path(dir, index))
            {
                let _ = f.set_len(0);
                let _ = f.write_all(info.as_bytes());
            }
            return Some(guard);
        }
    }
    None
}

/// Take the first free slot (not supported on this platform: nothing is gated).
#[cfg(not(unix))]
pub fn try_acquire_slot(_dir: &Path, _count: usize, _job: &str) -> Option<SlotGuard> {
    None
}

// --- admission -------------------------------------------------------------------------------

/// The wait ended because the process was asked to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped;

/// Sleep `duration`, waking early when `stop` is set. Returns `true` when stopped.
fn sleep_unless_stopped(duration: Duration, stop: &AtomicBool) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        std::thread::sleep((deadline - now).min(Duration::from_millis(50)));
    }
}

/// A job admitted past the gate: holds its slot (if it got one) until [`Admission::hold`] ends.
#[derive(Debug)]
pub struct Admission {
    guard: Option<SlotGuard>,
    release_file: PathBuf,
    timeout: Duration,
    since: Instant,
    /// How long admission took, jitter included.
    pub waited: Duration,
}

impl Admission {
    /// The slot taken (`None`: the wait timed out or no slots are configured).
    pub fn slot(&self) -> Option<usize> {
        self.guard.as_ref().map(SlotGuard::index)
    }

    /// The path [`signal_first_lap_done`] creates.
    pub fn release_file(&self) -> &Path {
        &self.release_file
    }

    /// The value of [`RELEASE_ENV`] for the worker.
    pub fn release_env(&self) -> String {
        format!("{}:{}", std::process::id(), self.release_file.display())
    }

    /// Hold the slot until the worker signals its first lap, `exited` is set, the timeout passes,
    /// or (with `settle` and the worker's PID, on Linux) the worker has been nearly idle for
    /// `settle`; then free it. `log` is told when the timeout or the idleness frees it.
    pub fn hold(
        self,
        exited: &AtomicBool,
        worker: Option<(i32, Duration)>,
        log: &mut dyn FnMut(&str),
    ) {
        /// Under this share of one core counts as idle.
        const IDLE_SHARE: f64 = 0.10;
        let mut guard = self.guard;
        let _ = fs::remove_file(&self.release_file);
        let release_file = self.release_file;
        let settle = worker.filter(|(_, d)| !d.is_zero());
        let mut last_sample = (
            Instant::now(),
            settle.and_then(|(pid, _)| tree_cpu_ticks(pid)),
        );
        let mut quiet_since: Option<Instant> = None;
        loop {
            if release_file.exists() || exited.load(Ordering::SeqCst) {
                break;
            }
            if self.since.elapsed() >= self.timeout {
                if guard.is_some() {
                    log(&format!(
                        "Startup: released the startup slot after {:.0} s without a finished first lap.",
                        self.timeout.as_secs_f64()
                    ));
                }
                break;
            }
            if let Some((pid, settle)) = settle
                && last_sample.0.elapsed() >= Duration::from_secs(1)
            {
                let now = Instant::now();
                let ticks = tree_cpu_ticks(pid);
                if let (Some(before), Some(after)) = (last_sample.1, ticks) {
                    let used = after.saturating_sub(before) as f64 / 100.0;
                    let share = used / now.duration_since(last_sample.0).as_secs_f64();
                    if share < IDLE_SHARE {
                        quiet_since.get_or_insert(last_sample.0);
                    } else {
                        quiet_since = None;
                    }
                }
                last_sample = (now, ticks);
                if quiet_since.is_some_and(|q| now.duration_since(q) >= settle) {
                    if guard.is_some() {
                        log(&format!(
                            "Startup: released the startup slot after {:.0} s: the job is idle.",
                            self.since.elapsed().as_secs_f64()
                        ));
                    }
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        guard.take();
        let _ = fs::remove_file(&release_file);
    }
}

/// [`admit_with`] with the real probes.
pub fn admit(
    policy: &StartupPolicy,
    job: &str,
    stop: &AtomicBool,
    log: &mut dyn FnMut(&str),
) -> Result<Admission, Stopped> {
    admit_with(policy, job, stop, log, &Probes::system())
}

/// Wait out the gate described by `policy` for `job` (see the module documentation). `stop` ends
/// the wait with [`Stopped`]; `log` receives what the job log should say.
pub fn admit_with(
    policy: &StartupPolicy,
    job: &str,
    stop: &AtomicBool,
    log: &mut dyn FnMut(&str),
    probes: &Probes,
) -> Result<Admission, Stopped> {
    let began = Instant::now();
    let timeout = Duration::from_secs_f64(policy.slot_timeout_seconds.max(0.0));
    if policy.jitter_seconds > 0.0 {
        let delay = Duration::from_secs_f64(random_unit() * policy.jitter_seconds);
        if delay >= Duration::from_millis(100) {
            log(&format!(
                "Startup: waiting {:.1} s before starting (startup jitter).",
                delay.as_secs_f64()
            ));
        }
        if sleep_unless_stopped(delay, stop) {
            return Err(Stopped);
        }
    }
    let wait_began = Instant::now();
    let mut last_note: Option<(String, Instant)> = None;
    let mut note = |reason: String, log: &mut dyn FnMut(&str)| {
        let repeat = last_note
            .as_ref()
            .is_some_and(|(r, at)| *r == reason && at.elapsed() < Duration::from_secs(60));
        if !repeat {
            log(&format!("Startup: waiting because {reason}."));
            last_note = Some((reason, Instant::now()));
        }
    };
    let guard = loop {
        if stop.load(Ordering::SeqCst) {
            return Err(Stopped);
        }
        let reason = pressure(policy, probes);
        if reason.is_none() {
            if policy.max_concurrent == 0 || !cfg!(unix) {
                break None;
            }
            if let Some(guard) = try_acquire_slot(&policy.dir, policy.max_concurrent, job) {
                break Some(guard);
            }
        }
        let waiting_for = reason.unwrap_or_else(|| "every startup slot is busy".into());
        if wait_began.elapsed() >= timeout {
            log(&format!(
                "Startup: gave up waiting after {:.0} s ({waiting_for}); starting anyway.",
                timeout.as_secs_f64()
            ));
            break None;
        }
        note(waiting_for, log);
        let poll = Duration::from_millis(200 + (random_unit() * 100.0) as u64);
        if sleep_unless_stopped(poll, stop) {
            return Err(Stopped);
        }
    };
    let release_file = policy.dir.join(format!("{}.release", std::process::id()));
    let _ = fs::create_dir_all(&policy.dir);
    let _ = fs::remove_file(&release_file);
    sweep_stale_releases(&policy.dir);
    if let Some(g) = &guard {
        let waited = began.elapsed();
        if waited >= Duration::from_millis(500) {
            log(&format!(
                "Startup: took startup slot {} after {:.1} s.",
                g.index(),
                waited.as_secs_f64()
            ));
        }
    }
    Ok(Admission {
        guard,
        release_file,
        timeout,
        since: Instant::now(),
        waited: began.elapsed(),
    })
}

/// Remove release files a crashed host left behind (older than a day).
fn sweep_stale_releases(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "release")
            && entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|m| m.elapsed().ok())
                .is_some_and(|age| age > Duration::from_secs(86_400))
        {
            let _ = fs::remove_file(path);
        }
    }
}

// --- the worker's side ------------------------------------------------------------------------

static FIRST_LAP_SIGNALLED: AtomicBool = AtomicBool::new(false);

/// Tell the host which holds this job's startup slot that the first lap is done (so the slot goes
/// to the next job). A no-op outside a gated job, when called again, and in a process which is
/// not the host's direct child (a command the job runs does not release the job's slot).
pub fn signal_first_lap_done() {
    if FIRST_LAP_SIGNALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    let Ok(value) = std::env::var(RELEASE_ENV) else {
        return;
    };
    let Some((host, path)) = value.split_once(':') else {
        return;
    };
    if host.parse::<u32>().ok() != parent_pid() {
        return;
    }
    let _ = fs::write(path, b"");
}

#[cfg(unix)]
fn parent_pid() -> Option<u32> {
    Some(nix::unistd::getppid().as_raw() as u32)
}

#[cfg(not(unix))]
fn parent_pid() -> Option<u32> {
    None
}

// --- explicit starts of `systemd` jobs --------------------------------------------------------

fn hint_path(dir: &Path, job: &str) -> PathBuf {
    let hex: String = job.bytes().map(|b| format!("{b:02x}")).collect();
    // A file name is at most 255 bytes: a long job name (a `systemd` unit name may be longer)
    // keeps its first bytes and gets a hash of the whole name.
    let name = if hex.len() <= 120 {
        hex
    } else {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in job.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("long-{hash:016x}-{}", &hex[..64])
    };
    dir.join("hints").join(name)
}

/// Record how the next start of `systemd` job `job` was classified (the unit wrapper reads it:
/// the unit cannot carry per-start environment).
pub fn write_start_hint(dir: &Path, job: &str, kind: StartKind) {
    let path = hint_path(dir, job);
    if kind == StartKind::Bulk {
        let _ = fs::remove_file(&path);
        return;
    }
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(&path, kind.as_str());
}

/// Read and remove the hint of [`write_start_hint`] (hints older than two minutes are ignored).
pub fn take_start_hint(dir: &Path, job: &str) -> Option<StartKind> {
    let path = hint_path(dir, job);
    let fresh = fs::metadata(&path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| m.elapsed().ok())
        .is_some_and(|age| age < HINT_MAX_AGE);
    let text = fs::read_to_string(&path).ok();
    let _ = fs::remove_file(&path);
    text.filter(|_| fresh).and_then(|t| StartKind::parse(&t))
}

#[cfg(test)]
mod tests {
    /// A fake `/proc` and cgroup tree.
    struct Fake {
        dir: tempfile::TempDir,
    }

    impl Fake {
        fn new() -> Fake {
            let fake = Fake {
                dir: tempfile::tempdir().unwrap(),
            };
            std::fs::create_dir_all(fake.dir.path().join("proc/self")).unwrap();
            std::fs::create_dir_all(fake.dir.path().join("cgroup")).unwrap();
            fake
        }
        fn write(&self, rel: &str, text: &str) {
            let path = self.dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
        fn paths(&self) -> SystemPaths {
            SystemPaths {
                proc_dir: self.dir.path().join("proc"),
                cgroup_dir: self.dir.path().join("cgroup"),
            }
        }
    }

    const MEMINFO: &str =
        "MemTotal:       16000000 kB\nMemFree:          100000 kB\nMemAvailable:    1600000 kB\n";

    #[test]
    fn the_host_cpu_count_is_read_from_proc_not_from_the_cgroup_limit() {
        let fake = Fake::new();
        let cpuinfo: String = (0..64)
            .map(|i| format!("processor\t: {i}\nmodel name\t: x\n\n"))
            .collect();
        fake.write("proc/cpuinfo", &cpuinfo);
        assert_eq!(host_cpu_count_in(&fake.paths()), 64);
        // Without cpuinfo, the `cpuN` lines of /proc/stat.
        let fake = Fake::new();
        fake.write("proc/stat", "cpu  1 2 3\ncpu0 1 2 3\ncpu1 1 2 3\nintr 5\n");
        assert_eq!(host_cpu_count_in(&fake.paths()), 2.max(cpu_count()));
        // Neither: this process's own count.
        assert_eq!(host_cpu_count_in(&Fake::new().paths()), cpu_count());
    }

    #[test]
    fn a_cgroup_memory_limit_replaces_the_hosts_available_memory() {
        // No cgroup limit: the host's MemAvailable (10%).
        let fake = Fake::new();
        fake.write("proc/meminfo", MEMINFO);
        fake.write("proc/self/cgroup", "0::/\n");
        fake.write("cgroup/memory.max", "max\n");
        fake.write("cgroup/memory.current", "123\n");
        assert_eq!(available_memory_percent_in(&fake.paths()), Some(10.0));

        // A container near its 1 GiB limit on a host with plenty: the container is what counts.
        let fake = Fake::new();
        fake.write(
            "proc/meminfo",
            "MemTotal: 16000000 kB\nMemAvailable: 12000000 kB\n",
        );
        fake.write("proc/self/cgroup", "0::/\n");
        fake.write("cgroup/memory.max", "1073741824\n");
        fake.write("cgroup/memory.current", "1000000000\n");
        fake.write("cgroup/memory.stat", "anon 1\ninactive_file 100000000\n");
        let free = available_memory_percent_in(&fake.paths()).unwrap();
        // (1073741824 - (1000000000 - 100000000)) / 1073741824
        assert!((free - 16.17).abs() < 0.1, "{free}");

        // ... and a container with room on a starved host is not delayed by the host.
        let fake = Fake::new();
        fake.write("proc/meminfo", MEMINFO);
        fake.write("proc/self/cgroup", "0::/\n");
        fake.write("cgroup/memory.max", "1073741824\n");
        fake.write("cgroup/memory.current", "107374182\n");
        let free = available_memory_percent_in(&fake.paths()).unwrap();
        assert!((free - 90.0).abs() < 0.01, "{free}");
    }

    #[test]
    fn the_tightest_limit_on_the_cgroup_path_counts_and_v1_is_read() {
        // A service inside a slice whose parent has the tighter limit (cgroup v2).
        let fake = Fake::new();
        fake.write("proc/meminfo", MEMINFO);
        fake.write("proc/self/cgroup", "0::/system.slice/job.service\n");
        fake.write("cgroup/system.slice/job.service/memory.max", "max\n");
        fake.write("cgroup/system.slice/job.service/memory.current", "1\n");
        fake.write("cgroup/system.slice/memory.max", "1000\n");
        fake.write("cgroup/system.slice/memory.current", "950\n");
        let free = available_memory_percent_in(&fake.paths()).unwrap();
        assert!((free - 5.0).abs() < 0.01, "{free}");

        // cgroup v1: a limit of 2000 with 500 in use; a limit that is "unlimited" is ignored.
        let fake = Fake::new();
        fake.write("proc/meminfo", MEMINFO);
        fake.write(
            "proc/self/cgroup",
            "4:memory:/docker/abc\n1:cpu:/docker/abc\n",
        );
        fake.write("cgroup/memory/docker/abc/memory.limit_in_bytes", "2000\n");
        fake.write("cgroup/memory/docker/abc/memory.usage_in_bytes", "500\n");
        fake.write(
            "cgroup/memory/memory.limit_in_bytes",
            "9223372036854771712\n",
        );
        fake.write("cgroup/memory/memory.usage_in_bytes", "9\n");
        let free = available_memory_percent_in(&fake.paths()).unwrap();
        assert!((free - 75.0).abs() < 0.01, "{free}");
    }

    use super::*;

    fn policy(dir: &Path, kind: StartKind, slots: usize) -> StartupPolicy {
        StartupPolicy {
            kind,
            jitter_seconds: 0.0,
            max_load: 0.0,
            min_free_memory_percent: 0.0,
            max_concurrent: slots,
            slot_timeout_seconds: 5.0,
            settle_seconds: 0.0,
            dir: dir.join(STARTUP_DIR),
        }
    }

    #[test]
    fn defaults_match_the_decision() {
        let c = StartupConfig::default();
        assert!(c.enabled);
        assert_eq!(c.jitter_seconds, 30.0);
        assert_eq!(c.max_load, 1.5);
        assert_eq!(c.min_free_memory_percent, 10.0);
        assert_eq!(c.slot_timeout_seconds, 300.0);
        assert_eq!(c.settle_seconds, 15.0);
        assert_eq!(c.slots(1), 2);
        assert_eq!(c.slots(4), 2);
        assert_eq!(c.slots(16), 8);
        let mut c = c;
        c.max_concurrent = Some(3);
        assert_eq!(c.slots(16), 3);
    }

    #[test]
    fn overrides_patch_the_config_and_ignore_bad_values() {
        let mut c = StartupConfig::default();
        let over = Value::from_json_str(
            r#"{"jitter_seconds": 5, "max_load": -1, "max_concurrent": 1, "slot_timeout_seconds": "x", "min_free_memory_percent": true}"#,
        )
        .unwrap();
        c.apply(over.as_map().unwrap());
        assert_eq!(c.jitter_seconds, 5.0);
        assert_eq!(c.max_load, 1.5);
        assert_eq!(c.max_concurrent, Some(1));
        assert_eq!(c.slot_timeout_seconds, 300.0);
        assert_eq!(c.min_free_memory_percent, 10.0);
        let off = Value::from_json_str(r#"{"stagger": false}"#).unwrap();
        c.apply(off.as_map().unwrap());
        assert!(!c.enabled);
    }

    #[test]
    fn the_policy_follows_the_kind() {
        let dir = Path::new("/r/jobs");
        let config = StartupConfig::default();
        let bulk = StartupPolicy::resolve(&config, None, StartKind::Bulk, dir).unwrap();
        assert_eq!(bulk.jitter_seconds, 30.0);
        assert_eq!(bulk.max_load, 1.5);
        assert_eq!(bulk.dir, dir.join(".startup"));
        let single = StartupPolicy::resolve(&config, None, StartKind::Single, dir).unwrap();
        assert_eq!(single.jitter_seconds, 0.0);
        assert_eq!(single.max_load, 0.0);
        assert_eq!(single.min_free_memory_percent, 0.0);
        assert!(single.max_concurrent >= 2);
        assert!(StartupPolicy::resolve(&config, None, StartKind::None, dir).is_none());
        let off = Value::from_json_str(r#"{"stagger": false}"#).unwrap();
        assert!(StartupPolicy::resolve(&config, off.as_map(), StartKind::Bulk, dir).is_none());
        let slow = Value::from_json_str(r#"{"jitter_seconds": 7}"#).unwrap();
        let p = StartupPolicy::resolve(&config, slow.as_map(), StartKind::Bulk, dir).unwrap();
        assert_eq!(p.jitter_seconds, 7.0);
    }

    #[test]
    fn the_policy_round_trips_as_an_argument() {
        let p = StartupPolicy {
            kind: StartKind::Bulk,
            jitter_seconds: 2.5,
            max_load: 1.5,
            min_free_memory_percent: 10.0,
            max_concurrent: 3,
            slot_timeout_seconds: 60.0,
            settle_seconds: 15.0,
            dir: PathBuf::from("/r/jobs/.startup"),
        };
        assert_eq!(StartupPolicy::from_arg(&p.to_arg()).unwrap(), p);
        assert!(StartupPolicy::from_arg("{").is_err());
        assert!(StartupPolicy::from_arg("{}").is_err());
    }

    #[test]
    fn kinds_parse() {
        assert_eq!(StartKind::parse("Bulk"), Some(StartKind::Bulk));
        assert_eq!(StartKind::parse(" single "), Some(StartKind::Single));
        assert_eq!(StartKind::parse("no-stagger"), Some(StartKind::None));
        assert_eq!(StartKind::parse("x"), None);
    }

    #[test]
    fn offsets_are_fixed_per_name_and_bounded() {
        let a = fixed_offset("job-a", 30.0);
        assert_eq!(a, fixed_offset("job-a", 30.0));
        assert!(a <= Duration::from_secs(30));
        // FNV-1a is stable: these never change between releases.
        assert_eq!(fixed_offset("job-a", 30.0), fixed_offset("job-a", 30.0));
        assert_eq!(
            fixed_offset("", 30.0),
            Duration::from_millis(0xcbf2_9ce4_8422_2325 % 30_001)
        );
        assert_eq!(fixed_offset("x", 0.0), Duration::ZERO);
        assert_eq!(fixed_offset("x", f64::NAN), Duration::ZERO);
        let spread: std::collections::HashSet<u128> = (0..50)
            .map(|i| fixed_offset(&format!("job-{i}"), 30.0).as_millis())
            .collect();
        assert!(spread.len() > 40, "offsets collapse: {}", spread.len());
    }

    #[test]
    fn pressure_reports_load_and_memory() {
        let p = StartupPolicy {
            max_load: 1.5,
            min_free_memory_percent: 10.0,
            ..policy(Path::new("/x"), StartKind::Bulk, 2)
        };
        let calm = Probes {
            load: || Some(1.0),
            memory: || Some(50.0),
            ncpu: || 2,
        };
        assert!(pressure(&p, &calm).is_none());
        let busy = Probes {
            load: || Some(3.5),
            ..calm
        };
        let reason = pressure(&p, &busy).unwrap();
        assert!(reason.contains("load average is 3.50"), "{reason}");
        let low = Probes {
            memory: || Some(4.0),
            ..calm
        };
        assert!(pressure(&p, &low).unwrap().contains("4.0% of memory"));
        // Missing probes pass (macOS memory, Windows).
        let blind = Probes {
            load: || None,
            memory: || None,
            ..calm
        };
        assert!(pressure(&p, &blind).is_none());
        // Zero disables a check.
        let off = StartupPolicy {
            max_load: 0.0,
            min_free_memory_percent: 0.0,
            ..p
        };
        assert!(pressure(&off, &busy).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn slots_are_exclusive_and_freed_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let slots = dir.path().join(STARTUP_DIR);
        let a = try_acquire_slot(&slots, 2, "a").unwrap();
        let b = try_acquire_slot(&slots, 2, "b").unwrap();
        assert_ne!(a.index(), b.index());
        assert!(try_acquire_slot(&slots, 2, "c").is_none());
        let freed = a.index();
        drop(a);
        let c = try_acquire_slot(&slots, 2, "c").unwrap();
        assert_eq!(c.index(), freed);
        let holder = fs::read_to_string(slot_path(&slots, freed)).unwrap();
        assert_eq!(holder, format!("{} c\n", std::process::id()));
        drop((b, c));
    }

    #[cfg(unix)]
    #[test]
    fn admission_waits_for_a_slot_then_proceeds() {
        let dir = tempfile::tempdir().unwrap();
        let p = policy(dir.path(), StartKind::Single, 1);
        let stop = AtomicBool::new(false);
        let mut lines = Vec::new();
        let first = admit(&p, "first", &stop, &mut |l| lines.push(l.to_string())).unwrap();
        assert_eq!(first.slot(), Some(0));
        // A second job waits until the first one's first lap is signalled.
        let release = first.release_file().to_path_buf();
        let p2 = p.clone();
        let waiter = std::thread::spawn(move || {
            let stop = AtomicBool::new(false);
            let started = Instant::now();
            let admitted = admit(&p2, "second", &stop, &mut |_| {}).unwrap();
            (started.elapsed(), admitted.slot())
        });
        std::thread::sleep(Duration::from_millis(600));
        let exited = AtomicBool::new(false);
        let holder = std::thread::spawn(move || {
            first.hold(&exited, None, &mut |_| {});
        });
        std::thread::sleep(Duration::from_millis(200));
        fs::write(&release, b"").unwrap();
        holder.join().unwrap();
        let (waited, slot) = waiter.join().unwrap();
        assert_eq!(slot, Some(0));
        assert!(waited >= Duration::from_millis(600), "{waited:?}");
    }

    #[cfg(unix)]
    #[test]
    fn a_slot_times_out_and_a_waiter_gives_up() {
        let dir = tempfile::tempdir().unwrap();
        let mut p = policy(dir.path(), StartKind::Single, 1);
        p.slot_timeout_seconds = 0.6;
        let stop = AtomicBool::new(false);
        let holder = admit(&p, "holder", &stop, &mut |_| {}).unwrap();
        // The holder never signals and never exits: the waiter gives up after the timeout.
        let mut lines = Vec::new();
        let started = Instant::now();
        let waiter = admit(&p, "waiter", &stop, &mut |l| lines.push(l.to_string())).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(600));
        assert!(waiter.slot().is_none());
        assert!(
            lines.iter().any(|l| l.contains("gave up waiting")),
            "{lines:?}"
        );
        // The holder frees its slot by itself at the timeout.
        let exited = AtomicBool::new(false);
        let mut freed = Vec::new();
        holder.hold(&exited, None, &mut |l| freed.push(l.to_string()));
        assert!(
            freed
                .iter()
                .any(|l| l.contains("released the startup slot"))
        );
        assert!(try_acquire_slot(&p.dir, 1, "next").is_some());
    }

    #[test]
    fn a_stop_ends_every_wait() {
        let dir = tempfile::tempdir().unwrap();
        let mut p = policy(dir.path(), StartKind::Bulk, 1);
        p.jitter_seconds = 60.0;
        let stop = AtomicBool::new(true);
        assert_eq!(admit(&p, "x", &stop, &mut |_| {}).unwrap_err(), Stopped);
    }

    #[test]
    fn pressure_delays_admission_until_it_clears() {
        use std::sync::atomic::AtomicUsize;
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        let dir = tempfile::tempdir().unwrap();
        let mut p = policy(dir.path(), StartKind::Bulk, 0);
        p.max_load = 1.0;
        let probes = Probes {
            load: || {
                // Busy for the first three probes.
                if CALLS.fetch_add(1, Ordering::SeqCst) < 3 {
                    Some(9.0)
                } else {
                    Some(0.1)
                }
            },
            memory: || None,
            ncpu: || 1,
        };
        let stop = AtomicBool::new(false);
        let mut lines = Vec::new();
        let started = Instant::now();
        let admitted =
            admit_with(&p, "x", &stop, &mut |l| lines.push(l.to_string()), &probes).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(400));
        assert!(admitted.slot().is_none());
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.contains("load average is 9.00"))
                .count(),
            1,
            "the same reason is logged once: {lines:?}"
        );
    }

    #[test]
    fn a_long_job_name_still_gets_a_start_hint() {
        let dir = tempfile::tempdir().unwrap();
        let long = "j".repeat(300);
        write_start_hint(dir.path(), &long, StartKind::Single);
        assert_eq!(take_start_hint(dir.path(), &long), Some(StartKind::Single));
        // A name that shares the prefix has its own hint.
        let other = format!("{}x", "j".repeat(299));
        write_start_hint(dir.path(), &long, StartKind::Single);
        assert_eq!(take_start_hint(dir.path(), &other), None);
    }

    #[test]
    fn start_hints_are_read_once_and_expire() {
        let dir = tempfile::tempdir().unwrap();
        write_start_hint(dir.path(), "my job", StartKind::Single);
        assert_eq!(
            take_start_hint(dir.path(), "my job"),
            Some(StartKind::Single)
        );
        assert_eq!(take_start_hint(dir.path(), "my job"), None);
        write_start_hint(dir.path(), "my job", StartKind::None);
        write_start_hint(dir.path(), "my job", StartKind::Bulk);
        assert_eq!(take_start_hint(dir.path(), "my job"), None);
        write_start_hint(dir.path(), "old", StartKind::Single);
        let path = hint_path(dir.path(), "old");
        let old = std::time::SystemTime::now() - Duration::from_secs(600);
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert_eq!(take_start_hint(dir.path(), "old"), None);
    }
}
