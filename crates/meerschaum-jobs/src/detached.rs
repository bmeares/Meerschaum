//! Finding a job's process without its PID file, without scanning the process table over and
//! over.
//!
//! A job's state comes from what was recorded about it (`process.began` / `ended` / `launched` in
//! `properties.json`, the PID file). Only a job which *began and never ended* but has no PID file
//! may still be running as a detached process (the file was lost, or the process was killed
//! before it recorded its end), and the only way to find such a process is to look at every
//! process's command line. That is expensive on a busy host, and a completed job never has a PID
//! file, so asking for the PID of every retained job on every supervisor pass used to cost most
//! of a core (production profile of a v4.1.4 API).
//!
//! - [`Timestamps`] decides from metadata alone whether a search can be needed at all: a job whose
//!   last run ended, or which never began, is never searched for.
//! - One scan of the process table is shared by every job for a short time ([`INDEX_TTL`]) and
//!   indexed by `(daemon_id, jobs_dir)`; a job which was not found is not looked for again for
//!   [`NEGATIVE_TTL`] unless its recorded timestamps change.
//! - Starting, stopping, pausing, resuming and deleting a job forget what is cached for it.
//! - A Python (v4) daemon has no `--jobs-dir` on its command line, so it matches only when the
//!   `MRSM_ROOT_DIR` it was started with is this root (unset: the default root): two roots may run
//!   jobs with the same name. An unreadable environment or a relative root still matches (v4.1.6).
//! - A recovered process must have started no later than the job's `began` (plus a small slack),
//!   so a reused PID is not mistaken for it.
//!
//! The defaults can be changed under `jobs:detached:` (`index_seconds`, `negative_seconds`,
//! `launch_grace_seconds`, `create_time_slack_seconds`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::NaiveDateTime;
use meerschaum_core::value::{Map, Value};

use crate::process;

/// How long one scan of the process table is shared between jobs.
pub const INDEX_TTL: Duration = Duration::from_secs(2);
/// How long a job which was not found is not looked for again.
pub const NEGATIVE_TTL: Duration = Duration::from_secs(60);
/// After a launch the PID file may not exist yet, and nothing is remembered as missing.
pub const LAUNCH_GRACE: Duration = Duration::from_secs(30);
/// How much later than `began` a process may have started and still be the job's.
pub const CREATE_TIME_SLACK: Duration = Duration::from_secs(5);

/// The tunables (see the module documentation).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limits {
    /// [`INDEX_TTL`].
    pub index_ttl: Duration,
    /// [`NEGATIVE_TTL`].
    pub negative_ttl: Duration,
    /// [`LAUNCH_GRACE`].
    pub launch_grace: Duration,
    /// [`CREATE_TIME_SLACK`].
    pub create_slack: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            index_ttl: INDEX_TTL,
            negative_ttl: NEGATIVE_TTL,
            launch_grace: LAUNCH_GRACE,
            create_slack: CREATE_TIME_SLACK,
        }
    }
}

/// `process.began`, `ended` and `launched` of a job, as recorded (naive UTC), with the raw texts
/// that identify a change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Timestamps {
    /// The supervisor recorded that the process began.
    pub began: Option<NaiveDateTime>,
    /// The supervisor recorded that it ended.
    pub ended: Option<NaiveDateTime>,
    /// Meerschaum recorded that it launched the job (v4.1.5 writes the same key).
    pub launched: Option<NaiveDateTime>,
    raw: [Option<String>; 3],
}

fn parse_naive(text: &str) -> Option<NaiveDateTime> {
    // `datetime.isoformat()` omits the fraction when it is zero.
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(text.trim(), f).ok())
}

impl Timestamps {
    /// From a job's properties.
    pub fn from_properties(props: &Map) -> Timestamps {
        let process = props.get("process");
        let get = |key: &str| -> Option<String> {
            process
                .and_then(|p| p.get(key))
                .filter(|v| !v.is_null())
                .map(Value::py_str)
        };
        let raw = [get("began"), get("ended"), get("launched")];
        let parse = |i: usize| raw[i].as_deref().and_then(parse_naive);
        Timestamps {
            began: parse(0),
            ended: parse(1),
            launched: parse(2),
            raw,
        }
    }

    /// The last run has ended: `ended` is recorded and nothing began or was launched after it
    /// (a restarted job has `began` / `launched` later than its stale `ended`).
    pub fn is_completed(&self) -> bool {
        let Some(ended) = self.ended else {
            return false;
        };
        match self.began.into_iter().chain(self.launched).max() {
            None => true,
            Some(started) => ended >= started,
        }
    }

    /// Nothing was ever recorded about a run: there is no process to look for.
    pub fn never_ran(&self) -> bool {
        self.began.is_none() && self.launched.is_none()
    }

    /// Launched recently and the new process has not recorded `began` yet, so its PID file may
    /// simply not exist yet.
    pub fn in_launch_window(&self, now: NaiveDateTime, grace: Duration) -> bool {
        let Some(launched) = self.launched else {
            return false;
        };
        self.began.is_none_or(|began| began < launched)
            && (now - launched).to_std().is_ok_and(|age| age < grace)
    }

    /// `began` when it belongs to the current launch: the process which recorded it is the job's.
    pub fn current_began(&self) -> Option<NaiveDateTime> {
        let began = self.began?;
        self.launched.is_none_or(|l| began >= l).then_some(began)
    }

    fn fingerprint(&self) -> [Option<String>; 3] {
        self.raw.clone()
    }
}

/// What a supervisor found by its command line stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// Its worker: the job's PID.
    Worker(i32),
    /// The supervisor is finishing after its worker exited: the job is stopping, which reads as
    /// stopped.
    Stopping,
    /// The supervisor stands in for the job (the worker is not up yet, or the kernel does not
    /// say which process is the worker): nothing is written down.
    Supervisor(i32),
}

/// Resolve a supervisor to the job's process from its `children` (`None`: unreadable).
///
/// Without `children` files (kernels built without `CONFIG_PROC_CHILDREN`) nothing is known
/// about the worker, so the supervisor reads as running, as before workers were resolved: a
/// restart check must not launch a job a second time.
pub fn resolve_supervisor(
    supervisor: i32,
    children: Option<&[i32]>,
    began_recorded: bool,
) -> Resolved {
    match children {
        None => Resolved::Supervisor(supervisor),
        Some([worker]) => Resolved::Worker(*worker),
        // No worker: not up yet before `began`; after it the job is stopping.
        Some(_) if began_recorded => Resolved::Stopping,
        Some(_) => Resolved::Supervisor(supervisor),
    }
}

/// A process whose command line carries `daemon_id='<id>'`.
#[derive(Debug, Clone)]
struct Entry {
    pid: i32,
    daemon_id: String,
    /// `--jobs-dir <dir>` of a Rust supervisor; a Python daemon has none.
    jobs_dir: Option<String>,
    /// A Python daemon's root directory, from its environment (`None`: unknown, matches any root).
    root: Option<PathBuf>,
}

#[derive(Debug)]
struct Index {
    taken: Instant,
    /// When the scan began (naive UTC): a job launched later needs a newer one.
    began: NaiveDateTime,
    entries: Vec<Entry>,
}

type Key = (String, PathBuf);

#[derive(Default)]
struct State {
    index: Option<Index>,
    negative: HashMap<Key, (Instant, [Option<String>; 3])>,
}

static STATE: std::sync::LazyLock<Mutex<State>> =
    std::sync::LazyLock::new(|| Mutex::new(State::default()));

fn entries_of(pid: i32, args: &[String]) -> Vec<Entry> {
    const PREFIX: &str = "daemon_id='";
    let jobs_dir = args
        .iter()
        .position(|a| a == "--jobs-dir")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let mut out = Vec::new();
    let mut root = None;
    for arg in args {
        let mut rest = arg.as_str();
        while let Some(at) = rest.find(PREFIX) {
            rest = &rest[at + PREFIX.len()..];
            if let Some(end) = rest.find('\'') {
                if jobs_dir.is_none() && root.is_none() {
                    root = Some(process::process_environ(pid).and_then(|env| legacy_root(&env)));
                }
                out.push(Entry {
                    pid,
                    daemon_id: rest[..end].to_string(),
                    jobs_dir: jobs_dir.clone(),
                    root: root.clone().flatten(),
                });
            }
        }
    }
    out
}

fn scan() -> Index {
    let began = crate::pytime::utcnow().naive_utc();
    let me = std::process::id() as i32;
    let entries = process::process_cmdlines()
        .into_iter()
        .filter(|(pid, _)| *pid != me)
        .flat_map(|(pid, args)| entries_of(pid, &args))
        .collect();
    Index {
        taken: Instant::now(),
        began,
        entries,
    }
}

fn same_dir(a: &Path, b: &Path) -> bool {
    a == b || matches!((a.canonicalize(), b.canonicalize()), (Ok(x), Ok(y)) if x == y)
}

/// The root directory a Python daemon was started with: its absolute `MRSM_ROOT_DIR`, or the
/// default root (`$XDG_CONFIG_HOME/meerschaum`, `~/.config/meerschaum`) when unset. `None` for a
/// relative root (it depends on the directory the daemon was started from) or no home directory.
fn legacy_root(env: &[(String, String)]) -> Option<PathBuf> {
    let var = |name: &str| {
        env.iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| PathBuf::from(v))
    };
    if let Some(root) = var("MRSM_ROOT_DIR") {
        return root.is_absolute().then_some(root);
    }
    let dot_config = var("XDG_CONFIG_HOME")
        .filter(|p| p.is_absolute())
        .or_else(|| var("HOME").map(|home| home.join(".config")))?;
    Some(dot_config.join("meerschaum"))
}

/// A process which references a job on its command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Found {
    /// Its PID.
    pub pid: i32,
    /// A Rust supervisor (it was started with `--jobs-dir`): the worker doing the job's work is
    /// its child. A Python daemon is the worker itself.
    pub supervisor: bool,
}

/// The processes whose command line references this job (a Rust supervisor must
/// have been started for `jobs_dir`). The process table is read only when the shared index is
/// older than `limits.index_ttl`, predates `not_older_than`, or `fresh` is set.
pub fn find_pids(
    daemon_id: &str,
    jobs_dir: &Path,
    limits: &Limits,
    not_older_than: Option<NaiveDateTime>,
    fresh: bool,
) -> Vec<Found> {
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    let stale = match &state.index {
        None => true,
        Some(i) => {
            fresh
                || i.taken.elapsed() > limits.index_ttl
                || not_older_than.is_some_and(|t| i.began < t)
        }
    };
    if stale {
        state.index = Some(scan());
    }
    state
        .index
        .iter()
        .flat_map(|i| i.entries.iter())
        .filter(|e| {
            e.daemon_id == daemon_id
                && match (&e.jobs_dir, &e.root) {
                    (Some(d), _) => same_dir(Path::new(d), jobs_dir),
                    (None, Some(root)) => same_dir(&root.join("jobs"), jobs_dir),
                    (None, None) => true,
                }
        })
        .map(|e| Found {
            pid: e.pid,
            supervisor: e.jobs_dir.is_some(),
        })
        .collect()
}

/// Forget everything cached (tests start from a clean slate).
pub fn reset() {
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    state.index = None;
    state.negative.clear();
}

/// Forget the shared scan and what is remembered as missing for this job.
pub fn invalidate(daemon_id: &str, jobs_dir: &Path) {
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    state.index = None;
    state
        .negative
        .remove(&(daemon_id.to_string(), jobs_dir.to_path_buf()));
}

/// Whether a scan recently failed to find the job and nothing recorded about it has changed.
pub fn is_known_missing(
    daemon_id: &str,
    jobs_dir: &Path,
    timestamps: &Timestamps,
    limits: &Limits,
) -> bool {
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    let key = (daemon_id.to_string(), jobs_dir.to_path_buf());
    match state.negative.get(&key) {
        None => false,
        Some((at, fingerprint)) => {
            if *fingerprint == timestamps.fingerprint() && at.elapsed() <= limits.negative_ttl {
                true
            } else {
                state.negative.remove(&key);
                false
            }
        }
    }
}

/// Remember that the job could not be found.
pub fn remember_missing(daemon_id: &str, jobs_dir: &Path, timestamps: &Timestamps) {
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    state.negative.insert(
        (daemon_id.to_string(), jobs_dir.to_path_buf()),
        (Instant::now(), timestamps.fingerprint()),
    );
}

/// Whether `pid` cannot be the process which recorded `began`: it started more than `slack`
/// after it (a reused PID). An unreadable start time is not stale.
pub fn is_stale_process(pid: i32, began: NaiveDateTime, slack: Duration) -> bool {
    match process::process_start_time(pid) {
        Some(started) => (started - began).to_std().is_ok_and(|after| after > slack),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(began: Option<&str>, ended: Option<&str>, launched: Option<&str>) -> Map {
        let mut process = Map::new();
        for (k, v) in [("began", began), ("ended", ended), ("launched", launched)] {
            if let Some(v) = v {
                process.insert(k.into(), Value::Str(v.into()));
            }
        }
        let mut m = Map::new();
        m.insert("process".into(), Value::Map(process));
        m
    }

    #[test]
    fn completion_comes_from_the_recorded_timestamps() {
        let t = |b, e, l| Timestamps::from_properties(&props(b, e, l));
        assert!(
            t(
                Some("2026-01-01T00:00:00"),
                Some("2026-01-01T00:00:05.5"),
                None
            )
            .is_completed()
        );
        // restarted: began later than the stale `ended`
        assert!(
            !t(
                Some("2026-01-01T00:01:00"),
                Some("2026-01-01T00:00:05"),
                None
            )
            .is_completed()
        );
        // launched (a v4.1.5 key) later than `ended`: a restart which has not begun yet
        assert!(
            !t(
                Some("2026-01-01T00:00:00"),
                Some("2026-01-01T00:00:05"),
                Some("2026-01-01T00:02:00")
            )
            .is_completed()
        );
        assert!(!t(Some("2026-01-01T00:00:00"), None, None).is_completed());
        assert!(t(None, None, None).never_ran());
    }

    #[test]
    fn the_launch_window_ends_when_the_process_records_began() {
        let now = parse_naive("2026-01-01T00:00:10").unwrap();
        let grace = Duration::from_secs(30);
        let t = |b, l| Timestamps::from_properties(&props(b, None, l));
        assert!(t(None, Some("2026-01-01T00:00:05")).in_launch_window(now, grace));
        assert!(
            t(Some("2026-01-01T00:00:01"), Some("2026-01-01T00:00:05"))
                .in_launch_window(now, grace)
        );
        assert!(
            !t(Some("2026-01-01T00:00:07"), Some("2026-01-01T00:00:05"))
                .in_launch_window(now, grace)
        );
        assert!(!t(None, Some("2025-12-31T00:00:05")).in_launch_window(now, grace));
        assert!(!t(None, None).in_launch_window(now, grace));
    }

    #[test]
    fn a_supervisor_resolves_to_its_worker_or_stands_in() {
        use Resolved::*;
        // The worker is the supervisor's only child.
        assert_eq!(resolve_supervisor(10, Some(&[11]), true), Worker(11));
        // Without a worker: not up yet before `began`, stopping after it.
        assert_eq!(resolve_supervisor(10, Some(&[]), false), Supervisor(10));
        assert_eq!(resolve_supervisor(10, Some(&[]), true), Stopping);
        // `children` unreadable (no `CONFIG_PROC_CHILDREN`): nothing is known, so the supervisor
        // stands in as running, whether or not `began` is recorded.
        assert_eq!(resolve_supervisor(10, None, true), Supervisor(10));
        assert_eq!(resolve_supervisor(10, None, false), Supervisor(10));
    }

    #[test]
    fn marker_parsing_reads_every_daemon_id() {
        let args = vec![
            "mrsm".to_string(),
            "daemon_id='a b'".to_string(),
            "--jobs-dir".to_string(),
            "/r/jobs".to_string(),
        ];
        let e = entries_of(7, &args);
        assert_eq!(e.len(), 1);
        assert_eq!(
            (e[0].daemon_id.as_str(), e[0].jobs_dir.as_deref()),
            ("a b", Some("/r/jobs"))
        );
        let py = vec![
            "python".to_string(),
            "-c".to_string(),
            "daemon = Daemon(daemon_id='x'); _daemons['x'] = daemon".to_string(),
        ];
        let e = entries_of(8, &py);
        assert_eq!(e.len(), 1);
        assert_eq!(
            (e[0].daemon_id.as_str(), e[0].jobs_dir.as_deref()),
            ("x", None)
        );
    }

    #[test]
    fn a_python_daemons_root_comes_from_its_environment() {
        let env = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let root = |pairs: &[(&str, &str)]| legacy_root(&env(pairs));
        assert_eq!(
            root(&[("MRSM_ROOT_DIR", "/a/root"), ("HOME", "/home/u")]),
            Some(PathBuf::from("/a/root"))
        );
        // A relative root depends on where the daemon started: unknown.
        assert_eq!(root(&[("MRSM_ROOT_DIR", "rel/root")]), None);
        // Unset: the default root.
        assert_eq!(
            root(&[("HOME", "/home/u")]),
            Some(PathBuf::from("/home/u/.config/meerschaum"))
        );
        assert_eq!(
            root(&[("XDG_CONFIG_HOME", "/xdg"), ("HOME", "/home/u")]),
            Some(PathBuf::from("/xdg/meerschaum"))
        );
        assert_eq!(root(&[]), None);
    }
}
