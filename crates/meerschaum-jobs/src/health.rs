//! Job health: the lap heartbeats a job's worker writes, and the health computed from them.
//!
//! A job that repeats its work (`--loop`, a `-s` schedule, a chain with `: --loop`, `--listen`)
//! records each lap in `jobs/<name>/laps.json`: when the run and the current lap began, when the
//! last lap ended, when one last succeeded or failed, how many failed in a row, and the cadence it
//! runs at. The supervisor (local jobs) and the unit wrapper (`systemd` jobs) hand the worker the
//! file's path in [`LAPS_PATH_ENV`]; a `systemd` job's file sits in its hidden sister job's
//! directory, so both executors read it the same way, and the API server includes the computed
//! health in its job metadata for `api:` executors.
//!
//! [`assess`] turns a record into a [`JobHealth`]:
//!
//! - `stopped`: the job is not running (or is paused);
//! - `stalled`: no lap has ended (and no progress was reported) for [`STALL_FACTOR`] times the
//!   expected interval: the cadence (`--min-seconds`, the `-s` period), or the last lap's
//!   duration when that is longer, and never less than [`MIN_STALL_SECONDS`]. A `--listen` job,
//!   whose syncs come when its source sends data, is stalled after [`LISTEN_STALL_SECONDS`]
//!   without one. A first lap (no lap has ended yet, so its length is unknown) is given at least
//!   [`FIRST_LAP_STALL_SECONDS`];
//! - `failing`: the last lap failed (`consecutive_failures` > 0);
//! - `healthy`: the last lap succeeded;
//! - `unknown`: nothing recorded yet (a first lap still running, a job that does not run laps,
//!   a job started by an older version).
//!
//! Writes are cheap and bounded: a failure, the first success after a failure and the first lap
//! of a run are written at once; other successes and progress at most once every
//! [`WRITE_INTERVAL`]. A failed write never fails the job.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use meerschaum_core::value::{Map, Value};
use serde_json::{Value as Json, json};

use crate::daemon::Status;

/// The variable naming the heartbeat file of the job this process runs.
pub const LAPS_PATH_ENV: &str = "MRSM_JOB_LAPS_PATH";

/// The PID of the process which started the job's worker (its supervisor or unit wrapper): only
/// that process's direct child records, never the processes the worker starts in turn (Python
/// workers, chain steps, compose commands), which inherit the environment.
pub const LAPS_PARENT_ENV: &str = "MRSM_JOB_LAPS_PARENT";

/// The heartbeat file in a job's directory.
pub const LAPS_FILE_NAME: &str = "laps.json";

/// How often a healthy loop writes its heartbeat at most.
pub const WRITE_INTERVAL: Duration = Duration::from_secs(5);

/// The expected interval of a `--listen` job (its syncs come when the source sends data).
pub const LISTEN_INTERVAL_SECONDS: f64 = 3600.0;

/// A `--listen` job is stalled after this long without a sync (a fixed threshold: no
/// [`STALL_FACTOR`]).
pub const LISTEN_STALL_SECONDS: f64 = 3600.0;

/// A job is stalled after this many expected intervals without a lap.
pub const STALL_FACTOR: f64 = 3.0;

/// A job is never stalled sooner than this.
pub const MIN_STALL_SECONDS: f64 = 60.0;

/// A first lap (no lap of the job has ended yet: how long one takes is unknown, and a first sync
/// often backfills) is never stalled sooner than this.
pub const FIRST_LAP_STALL_SECONDS: f64 = 3600.0;

/// The longest message a record keeps.
const MAX_MESSAGE_CHARS: usize = 500;

/// Set while a [`Heartbeat`] of this process records: a lap loop nested in another (a chain step
/// that loops, a scheduled `--loop`) leaves the record to the outer one.
static CLAIMED: AtomicBool = AtomicBool::new(false);

/// Set in processes that must never record (the child of a timed chain step, an in-process
/// compose command): they inherit the job's environment.
static DISABLED: AtomicBool = AtomicBool::new(false);

/// Stop this process from recording heartbeats (it is a helper of the job's worker).
pub fn disable() {
    DISABLED.store(true, Ordering::SeqCst);
}

/// What a job's worker records about its laps.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LapRecord {
    /// The worker's PID.
    pub pid: Option<i64>,
    /// When this run of the worker began recording.
    pub run_started: Option<DateTime<Utc>>,
    /// When the current (or last) lap began.
    pub lap_started: Option<DateTime<Utc>>,
    /// When the last lap ended.
    pub last_lap: Option<DateTime<Utc>>,
    /// When a lap last succeeded.
    pub last_success: Option<DateTime<Utc>>,
    /// When a lap last failed.
    pub last_failure: Option<DateTime<Utc>>,
    /// When the current lap last reported progress (a synced chunk or pipe, a stream flush).
    pub last_progress: Option<DateTime<Utc>>,
    /// When the next lap is due (after a sleep, a cooldown or a schedule's wait).
    pub next_lap: Option<DateTime<Utc>>,
    /// Laps that failed in a row (0 after a success).
    pub consecutive_failures: u64,
    /// Laps ended, over every run.
    pub laps: u64,
    /// How long the last lap took.
    pub lap_seconds: Option<f64>,
    /// The cadence the worker runs at (`--min-seconds`, the `-s` period, an hour for `--listen`).
    pub interval_seconds: Option<f64>,
    /// A fixed stall threshold instead of [`STALL_FACTOR`] times the interval (a `--listen` job:
    /// [`LISTEN_STALL_SECONDS`]).
    pub stall_seconds: Option<f64>,
    /// What a lap does, for messages: `sync` or `run`.
    pub noun: String,
    /// The last lap's message (truncated, credentials redacted).
    pub message: Option<String>,
}

fn iso(dt: Option<DateTime<Utc>>) -> Json {
    dt.map(|d| Json::String(crate::pytime::aware_isoformat(d)))
        .unwrap_or(Json::Null)
}

fn parse_dt(value: Option<&Json>) -> Option<DateTime<Utc>> {
    let text = value?.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|d| d.with_timezone(&Utc))
        .or_else(|| {
            crate::pytime::fromisoformat(text).map(|d| match d {
                meerschaum_core::pyfmt::PyDateTime::Aware(a) => a.with_timezone(&Utc),
                meerschaum_core::pyfmt::PyDateTime::Naive(n) => n.and_utc(),
            })
        })
}

impl LapRecord {
    /// The record as JSON (timestamps ISO-8601 UTC).
    pub fn to_json(&self) -> Json {
        json!({
            "pid": self.pid,
            "run_started": iso(self.run_started),
            "lap_started": iso(self.lap_started),
            "last_lap": iso(self.last_lap),
            "last_success": iso(self.last_success),
            "last_failure": iso(self.last_failure),
            "last_progress": iso(self.last_progress),
            "next_lap": iso(self.next_lap),
            "consecutive_failures": self.consecutive_failures,
            "laps": self.laps,
            "lap_seconds": self.lap_seconds,
            "interval_seconds": self.interval_seconds,
            "stall_seconds": self.stall_seconds,
            "noun": self.noun,
            "message": self.message,
        })
    }

    /// A record read back from JSON (unknown keys ignored, missing ones empty).
    pub fn from_json(value: &Json) -> Option<LapRecord> {
        let obj = value.as_object()?;
        let float = |key: &str| obj.get(key).and_then(Json::as_f64);
        let count = |key: &str| obj.get(key).and_then(Json::as_u64).unwrap_or(0);
        Some(LapRecord {
            pid: obj.get("pid").and_then(Json::as_i64),
            run_started: parse_dt(obj.get("run_started")),
            lap_started: parse_dt(obj.get("lap_started")),
            last_lap: parse_dt(obj.get("last_lap")),
            last_success: parse_dt(obj.get("last_success")),
            last_failure: parse_dt(obj.get("last_failure")),
            last_progress: parse_dt(obj.get("last_progress")),
            next_lap: parse_dt(obj.get("next_lap")),
            consecutive_failures: count("consecutive_failures"),
            laps: count("laps"),
            lap_seconds: float("lap_seconds"),
            interval_seconds: float("interval_seconds"),
            stall_seconds: float("stall_seconds"),
            noun: obj
                .get("noun")
                .and_then(Json::as_str)
                .unwrap_or("run")
                .to_string(),
            message: obj
                .get("message")
                .and_then(Json::as_str)
                .map(str::to_string),
        })
    }
}

/// The heartbeat file of the job named `name` under `jobs_dir` (a `systemd` job's is in its
/// hidden sister job's directory, which has the same path).
pub fn laps_path(jobs_dir: &Path, name: &str) -> PathBuf {
    jobs_dir.join(name).join(LAPS_FILE_NAME)
}

/// Read a heartbeat file (`None` when it is missing or unreadable).
pub fn read_record(path: &Path) -> Option<LapRecord> {
    let text = std::fs::read_to_string(path).ok()?;
    LapRecord::from_json(&serde_json::from_str(&text).ok()?)
}

/// Write a heartbeat file atomically (a temporary file renamed over it). The directory must
/// exist: a job whose directory was deleted is not recreated by its last heartbeat.
pub fn write_record(path: &Path, record: &LapRecord) -> std::io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::other("no directory"))?;
    if !dir.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the job's directory is gone",
        ));
    }
    let tmp = dir.join(format!(".{LAPS_FILE_NAME}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, record.to_json().to_string())?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

/// Records the laps of a loop (see the module documentation). Inactive outside a job, in a
/// process that [`disable`]d it, and when an outer loop of this process already records.
#[derive(Debug)]
pub struct Heartbeat {
    path: Option<PathBuf>,
    record: LapRecord,
    lap_began: Option<Instant>,
    last_write: Option<Instant>,
    pending: bool,
    owns_claim: bool,
}

impl Heartbeat {
    /// The heartbeat of this process's job ([`LAPS_PATH_ENV`]): `interval_seconds` is the loop's
    /// cadence (`None`: none to expect), `noun` what a lap does (`sync` or `run`).
    pub fn from_env(interval_seconds: Option<f64>, noun: &str) -> Heartbeat {
        let path = std::env::var_os(LAPS_PATH_ENV)
            .filter(|p| !p.is_empty())
            .map(PathBuf::from)
            .filter(|_| is_the_jobs_worker());
        Heartbeat::at(path, interval_seconds, noun)
    }

    /// A heartbeat writing to `path` (`None`: inactive).
    pub fn at(path: Option<PathBuf>, interval_seconds: Option<f64>, noun: &str) -> Heartbeat {
        let claimed = path.is_some()
            && !DISABLED.load(Ordering::SeqCst)
            && CLAIMED
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        let path = path.filter(|_| claimed);
        let mut record = path.as_deref().and_then(read_record).unwrap_or_default();
        record.pid = Some(i64::from(std::process::id()));
        record.run_started = Some(crate::pytime::utcnow());
        record.lap_started = None;
        record.last_progress = None;
        record.next_lap = None;
        record.interval_seconds = interval_seconds.filter(|s| s.is_finite() && *s >= 0.0);
        record.stall_seconds = None;
        record.noun = noun.to_string();
        let mut heartbeat = Heartbeat {
            path,
            record,
            lap_began: None,
            last_write: None,
            pending: false,
            owns_claim: claimed,
        };
        heartbeat.write();
        heartbeat
    }

    /// Whether this heartbeat writes anything.
    pub fn is_active(&self) -> bool {
        self.path.is_some()
    }

    /// The record as last updated (written or pending).
    pub fn record(&self) -> &LapRecord {
        &self.record
    }

    /// Change the cadence (a `--loop` job whose pipes only sync on their own schedules).
    pub fn set_interval(&mut self, seconds: Option<f64>) {
        let seconds = seconds.filter(|s| s.is_finite() && *s >= 0.0);
        if self.record.interval_seconds != seconds {
            self.record.interval_seconds = seconds;
            self.pending = true;
        }
    }

    /// Use a fixed stall threshold instead of [`STALL_FACTOR`] times the interval (a `--listen`
    /// job: [`LISTEN_STALL_SECONDS`]); `None`: the factor.
    pub fn set_stall_seconds(&mut self, seconds: Option<f64>) {
        let seconds = seconds.filter(|s| s.is_finite() && *s > 0.0);
        if self.record.stall_seconds != seconds {
            self.record.stall_seconds = seconds;
            self.pending = true;
        }
    }

    /// A lap began.
    pub fn lap_started(&mut self) {
        if !self.is_active() {
            return;
        }
        // `next_lap` stays: a lap that ends later than it supersedes it (see `assess`).
        self.lap_began = Some(Instant::now());
        self.record.lap_started = Some(crate::pytime::utcnow());
        self.record.last_progress = None;
        // The first lap of a run is written at once: health then knows the job reports laps.
        if self.record.last_lap.is_none_or(|l| l < self.started()) {
            self.write();
        } else {
            self.pending = true;
        }
    }

    fn started(&self) -> DateTime<Utc> {
        self.record
            .run_started
            .unwrap_or_else(crate::pytime::utcnow)
    }

    /// A lap ended (`success`, with its message).
    pub fn lap_finished(&mut self, success: bool, message: &str) {
        if !self.is_active() {
            return;
        }
        let now = crate::pytime::utcnow();
        let was_failing = self.record.consecutive_failures > 0;
        let first_of_run = self.record.last_lap.is_none_or(|l| l < self.started());
        self.record.lap_seconds = self.lap_began.take().map(|b| b.elapsed().as_secs_f64());
        if self.record.lap_started.is_none() {
            self.record.lap_started = Some(now);
        }
        self.record.last_lap = Some(now);
        self.record.laps += 1;
        self.record.message = Some(clean_message(message));
        if success {
            self.record.last_success = Some(now);
            self.record.consecutive_failures = 0;
        } else {
            self.record.last_failure = Some(now);
            self.record.consecutive_failures += 1;
        }
        self.pending = true;
        if !success || was_failing || first_of_run || self.due() {
            self.write();
        }
    }

    /// The loop waits `pause` before its next lap (`--min-seconds`, a cooldown after a failure,
    /// a schedule's next fire): the job is not stalled while the next lap is not due.
    pub fn waiting(&mut self, pause: Duration) {
        if !self.is_active() {
            return;
        }
        let pause = chrono::Duration::from_std(pause).unwrap_or(chrono::Duration::MAX);
        let next = crate::pytime::utcnow()
            .checked_add_signed(pause)
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        self.record.next_lap = Some(next);
        self.pending = true;
        // A long wait is written at once (so is the first of a run): until the next lap, the
        // file must say when it is due.
        if pause >= chrono::Duration::from_std(WRITE_INTERVAL).unwrap_or_default() || self.due() {
            self.write();
        }
    }

    /// The current lap made progress (a chunk or a pipe synced, a stream flushed).
    pub fn progress(&mut self) {
        if !self.is_active() {
            return;
        }
        self.record.last_progress = Some(crate::pytime::utcnow());
        self.pending = true;
        if self.due() {
            self.write();
        }
    }

    /// A successful sync outside a lap (a `--listen` callback sync or stream flush): a lap of
    /// its own.
    pub fn event(&mut self, success: bool, message: &str) {
        self.lap_finished(success, message);
    }

    /// Write anything not yet written.
    pub fn flush(&mut self) {
        if self.pending {
            self.write();
        }
    }

    fn due(&self) -> bool {
        self.last_write
            .is_none_or(|w| w.elapsed() >= WRITE_INTERVAL)
    }

    fn write(&mut self) {
        let Some(path) = &self.path else {
            return;
        };
        // Recording never fails or slows the job: a failed write is retried with the next lap.
        if write_record(path, &self.record).is_ok() {
            self.pending = false;
        }
        self.last_write = Some(Instant::now());
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.flush();
        if self.owns_claim {
            CLAIMED.store(false, Ordering::SeqCst);
        }
    }
}

/// Whether this process is the job's worker itself (see [`LAPS_PARENT_ENV`]).
fn is_the_jobs_worker() -> bool {
    let Some(parent) = std::env::var(LAPS_PARENT_ENV)
        .ok()
        .and_then(|p| p.trim().parse::<u32>().ok())
    else {
        return true;
    };
    #[cfg(unix)]
    {
        std::os::unix::process::parent_id() == parent
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        true
    }
}

fn clean_message(message: &str) -> String {
    let redacted = meerschaum_core::debug::redact_uris(message.trim());
    if redacted.chars().count() <= MAX_MESSAGE_CHARS {
        return redacted;
    }
    let mut text: String = redacted.chars().take(MAX_MESSAGE_CHARS).collect();
    text.push('…');
    text
}

/// A job's health state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    /// The last lap succeeded.
    Healthy,
    /// No lap for too long.
    Stalled,
    /// The last lap failed.
    Failing,
    /// Not running (or paused).
    Stopped,
    /// Nothing recorded yet.
    Unknown,
}

impl HealthState {
    /// `healthy`, `stalled`, `failing`, `stopped` or `unknown`.
    pub fn as_str(self) -> &'static str {
        match self {
            HealthState::Healthy => "healthy",
            HealthState::Stalled => "stalled",
            HealthState::Failing => "failing",
            HealthState::Stopped => "stopped",
            HealthState::Unknown => "unknown",
        }
    }

    /// The state named `text`.
    pub fn parse(text: &str) -> Option<HealthState> {
        Some(match text {
            "healthy" => HealthState::Healthy,
            "stalled" => HealthState::Stalled,
            "failing" => HealthState::Failing,
            "stopped" => HealthState::Stopped,
            "unknown" => HealthState::Unknown,
            _ => return None,
        })
    }
}

/// A job's health (`job.health` in Python; `health` in the API's job metadata).
#[derive(Debug, Clone, PartialEq)]
pub struct JobHealth {
    /// The state.
    pub state: HealthState,
    /// When a lap last succeeded (ISO-8601 UTC).
    pub last_success: Option<String>,
    /// When the last lap ended (ISO-8601 UTC).
    pub last_lap: Option<String>,
    /// Laps that failed in a row.
    pub consecutive_failures: u64,
    /// The interval laps are expected at (`None`: the job has no cadence).
    pub expected_interval_seconds: Option<f64>,
    /// What to show next to the state: `last good sync 2 min ago`, `last 3 laps failed`, ...
    pub message: String,
}

impl JobHealth {
    /// Health with nothing known.
    pub fn unknown(message: impl Into<String>) -> JobHealth {
        JobHealth {
            state: HealthState::Unknown,
            last_success: None,
            last_lap: None,
            consecutive_failures: 0,
            expected_interval_seconds: None,
            message: message.into(),
        }
    }

    /// The health of a job that is not running.
    pub fn stopped(paused: bool) -> JobHealth {
        JobHealth {
            state: HealthState::Stopped,
            message: if paused { "paused" } else { "stopped" }.into(),
            ..JobHealth::unknown("")
        }
    }

    /// As JSON (`{"state", "last_success", "last_lap", "consecutive_failures",
    /// "expected_interval_seconds", "message"}`).
    pub fn to_json(&self) -> Json {
        json!({
            "state": self.state.as_str(),
            "last_success": self.last_success,
            "last_lap": self.last_lap,
            "consecutive_failures": self.consecutive_failures,
            "expected_interval_seconds": self.expected_interval_seconds,
            "message": self.message,
        })
    }

    /// As a core map (for the Python façade).
    pub fn to_map(&self) -> Map {
        Value::from_json_str(&self.to_json().to_string())
            .map(|v| v.to_map())
            .unwrap_or_default()
    }

    /// Health read back from [`JobHealth::to_json`] (an API server's job metadata).
    pub fn from_value(value: &Value) -> Option<JobHealth> {
        let state = HealthState::parse(value.get("state")?.as_str()?)?;
        let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_string);
        Some(JobHealth {
            state,
            last_success: text("last_success"),
            last_lap: text("last_lap"),
            consecutive_failures: value
                .get("consecutive_failures")
                .and_then(Value::as_i64)
                .map_or(0, |n| n.max(0) as u64),
            expected_interval_seconds: value
                .get("expected_interval_seconds")
                .and_then(Value::as_f64),
            message: text("message").unwrap_or_default(),
        })
    }
}

/// `2 min`, `3 h`, `4 days`: a duration as a short phrase.
pub fn humanize_seconds(seconds: f64) -> String {
    let s = seconds.max(0.0);
    let (n, unit) = if s < 60.0 {
        (s.floor(), "s")
    } else if s < 3600.0 {
        ((s / 60.0).floor(), "min")
    } else if s < 86400.0 {
        ((s / 3600.0).floor(), "h")
    } else {
        let days = (s / 86400.0).floor();
        return format!("{days} day{}", if days == 1.0 { "" } else { "s" });
    };
    format!("{n} {unit}")
}

fn ago(now: DateTime<Utc>, then: DateTime<Utc>) -> String {
    let seconds = (now - then).num_milliseconds() as f64 / 1000.0;
    if seconds < 5.0 {
        return "just now".into();
    }
    format!("{} ago", humanize_seconds(seconds))
}

/// The health of a job from its heartbeat `record`, its `status`, when its current run `began`
/// (if known), at `now`.
pub fn assess(
    record: Option<&LapRecord>,
    status: Status,
    began: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> JobHealth {
    if status != Status::Running {
        let mut health = JobHealth::stopped(status == Status::Paused);
        if let Some(record) = record {
            health.last_success = record.last_success.map(crate::pytime::aware_isoformat);
            health.last_lap = record.last_lap.map(crate::pytime::aware_isoformat);
            health.consecutive_failures = record.consecutive_failures;
        }
        return health;
    }
    let Some(record) = record else {
        return JobHealth::unknown("no laps recorded");
    };
    let noun = if record.noun.is_empty() {
        "run"
    } else {
        record.noun.as_str()
    };
    let expected = record
        .interval_seconds
        .map(|cadence| cadence.max(record.lap_seconds.unwrap_or(0.0)));
    let mut health = JobHealth {
        state: HealthState::Unknown,
        last_success: record.last_success.map(crate::pytime::aware_isoformat),
        last_lap: record.last_lap.map(crate::pytime::aware_isoformat),
        consecutive_failures: record.consecutive_failures,
        expected_interval_seconds: expected,
        message: String::new(),
    };
    let last_good = record
        .last_success
        .map(|t| format!("last good {noun} {}", ago(now, t)));
    // The latest sign of life: a lap ended, progress within a lap, or this run beginning.
    let activity = [
        record.last_lap,
        record.last_progress,
        record.run_started,
        began,
    ]
    .into_iter()
    .flatten()
    .max();
    // A lap that is not due yet (the loop sleeps, cools down after a failure, or waits for its
    // schedule) is counted from when it is due.
    let activity = activity.map(|a| record.next_lap.filter(|n| *n > a).unwrap_or(a));
    // How long without a lap ending (or progress) is a stall: a fixed time (`--listen`), else
    // three expected intervals, at least a minute.
    let threshold = match (record.stall_seconds, expected) {
        (Some(fixed), _) => Some(fixed),
        (None, Some(expected)) => Some((STALL_FACTOR * expected).max(MIN_STALL_SECONDS)),
        (None, None) => None,
    };
    // No lap has ended yet: how long one takes is not known (a first sync often backfills).
    let first_lap = record.last_lap.is_none();
    let threshold = threshold.map(|t| {
        if first_lap {
            t.max(FIRST_LAP_STALL_SECONDS)
        } else {
            t
        }
    });
    if let (Some(threshold), Some(activity)) = (threshold, activity) {
        let idle = (now - activity).num_milliseconds() as f64 / 1000.0;
        if idle > threshold {
            health.state = HealthState::Stalled;
            health.message = last_good
                .unwrap_or_else(|| format!("no good {noun} in {}", humanize_seconds(idle)));
            return health;
        }
    }
    if record.consecutive_failures > 0 {
        health.state = HealthState::Failing;
        health.message = match record.consecutive_failures {
            1 => "last lap failed".to_string(),
            n => format!("last {n} laps failed"),
        };
        if let Some(good) = last_good {
            health.message.push_str(&format!("; {good}"));
        }
        return health;
    }
    match last_good {
        Some(good) => {
            health.state = HealthState::Healthy;
            health.message = good;
        }
        None if first_lap && record.lap_started.is_some() => {
            health.message = format!("first {noun} in progress");
        }
        None => health.message = "no laps yet".into(),
    }
    health
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A process has one recording heartbeat at a time: the tests that record take turns.
    static RECORDING: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn record(interval: Option<f64>) -> LapRecord {
        LapRecord {
            run_started: Some(at(0)),
            interval_seconds: interval,
            noun: "sync".into(),
            ..LapRecord::default()
        }
    }

    #[test]
    fn states_follow_laps_failures_and_time() {
        let mut r = record(Some(60.0));
        // Running, nothing ended yet: unknown until the stall threshold.
        let h = assess(Some(&r), Status::Running, None, at(30));
        assert_eq!(h.state, HealthState::Unknown);
        assert_eq!(h.message, "no laps yet");
        assert_eq!(h.expected_interval_seconds, Some(60.0));
        // A first lap under way: how long a lap takes is not known yet, so 3 × 60 s is not a
        // stall (a first sync often backfills)...
        r.lap_started = Some(at(0));
        let h = assess(Some(&r), Status::Running, None, at(181));
        assert_eq!(
            (h.state, h.message.as_str()),
            (HealthState::Unknown, "first sync in progress")
        );
        // ...an hour without the lap ending or reporting progress is.
        let h = assess(Some(&r), Status::Running, None, at(3599));
        assert_eq!(h.state, HealthState::Unknown);
        let h = assess(Some(&r), Status::Running, None, at(3601));
        assert_eq!(h.state, HealthState::Stalled);
        assert_eq!(h.message, "no good sync in 1 h");
        // A daily cadence keeps its own, longer threshold for a first lap.
        let mut daily_first = record(Some(86400.0));
        daily_first.lap_started = Some(at(0));
        assert_eq!(
            assess(Some(&daily_first), Status::Running, None, at(2 * 86400)).state,
            HealthState::Unknown
        );

        r.last_lap = Some(at(100));
        r.last_success = Some(at(100));
        r.laps = 1;
        let h = assess(Some(&r), Status::Running, None, at(220));
        assert_eq!(h.state, HealthState::Healthy);
        assert_eq!(h.message, "last good sync 2 min ago");
        assert_eq!(h.last_success.as_deref(), Some("2027-01-15T08:01:40+00:00"));

        r.last_lap = Some(at(200));
        r.last_failure = Some(at(200));
        r.consecutive_failures = 3;
        let h = assess(Some(&r), Status::Running, None, at(230));
        assert_eq!(h.state, HealthState::Failing);
        assert_eq!(h.message, "last 3 laps failed; last good sync 2 min ago");
        assert_eq!(h.consecutive_failures, 3);

        // Nothing for 3 h: stalled, whatever the last lap did.
        let h = assess(Some(&r), Status::Running, None, at(200 + 3 * 3600));
        assert_eq!(h.state, HealthState::Stalled);
        assert_eq!(h.message, "last good sync 3 h ago");

        // Progress within a long lap keeps it from looking stalled.
        r.consecutive_failures = 0;
        r.last_success = Some(at(200));
        r.last_progress = Some(at(10_000));
        let h = assess(Some(&r), Status::Running, None, at(10_100));
        assert_eq!(h.state, HealthState::Healthy);

        // Not running: stopped (or paused), with what was last recorded.
        let h = assess(Some(&r), Status::Stopped, None, at(10_100));
        assert_eq!(
            (h.state, h.message.as_str()),
            (HealthState::Stopped, "stopped")
        );
        assert!(h.last_success.is_some());
        let h = assess(Some(&r), Status::Paused, None, at(10_100));
        assert_eq!(
            (h.state, h.message.as_str()),
            (HealthState::Stopped, "paused")
        );
        assert_eq!(
            assess(None, Status::Running, None, at(0)).state,
            HealthState::Unknown
        );
    }

    #[test]
    fn the_threshold_follows_the_cadence_and_slow_laps() {
        // `--min-seconds 1`: never stalled within a minute.
        let mut r = record(Some(1.0));
        r.last_lap = Some(at(0));
        r.last_success = Some(at(0));
        assert_eq!(
            assess(Some(&r), Status::Running, None, at(59)).state,
            HealthState::Healthy
        );
        assert_eq!(
            assess(Some(&r), Status::Running, None, at(61)).state,
            HealthState::Stalled
        );
        // Laps of 10 minutes stretch the expected interval.
        r.lap_seconds = Some(600.0);
        let h = assess(Some(&r), Status::Running, None, at(1700));
        assert_eq!(h.state, HealthState::Healthy);
        assert_eq!(h.expected_interval_seconds, Some(600.0));
        assert_eq!(
            assess(Some(&r), Status::Running, None, at(1801)).state,
            HealthState::Stalled
        );
        // A daily schedule: three days.
        let mut daily = record(Some(86400.0));
        daily.last_lap = Some(at(0));
        daily.last_success = Some(at(0));
        assert_eq!(
            assess(Some(&daily), Status::Running, None, at(2 * 86400)).state,
            HealthState::Healthy
        );
        // No cadence (one lap, no loop): never stalled.
        let mut once = record(None);
        once.last_lap = Some(at(0));
        once.last_success = Some(at(0));
        assert_eq!(
            assess(Some(&once), Status::Running, None, at(10 * 86400)).state,
            HealthState::Healthy
        );
        // A restart: the new run's start counts as activity.
        let mut restarted = record(Some(60.0));
        restarted.last_lap = Some(at(0));
        restarted.last_success = Some(at(0));
        restarted.run_started = Some(at(5000));
        assert_eq!(
            assess(Some(&restarted), Status::Running, None, at(5100)).state,
            HealthState::Healthy
        );
    }

    #[test]
    fn a_listening_job_is_stalled_after_an_hour_without_a_sync() {
        // Q26: `--listen` jobs are stalled after an hour (not three of them).
        let mut r = record(Some(LISTEN_INTERVAL_SECONDS));
        r.stall_seconds = Some(LISTEN_STALL_SECONDS);
        r.last_lap = Some(at(0));
        r.last_success = Some(at(0));
        assert_eq!(
            assess(Some(&r), Status::Running, None, at(3599)).state,
            HealthState::Healthy
        );
        let h = assess(Some(&r), Status::Running, None, at(3601));
        assert_eq!(
            (h.state, h.message.as_str()),
            (HealthState::Stalled, "last good sync 1 h ago")
        );
        assert_eq!(h.expected_interval_seconds, Some(3600.0));
        // A stream flush (progress) keeps it from looking stalled.
        r.last_progress = Some(at(3000));
        assert_eq!(
            assess(Some(&r), Status::Running, None, at(6000)).state,
            HealthState::Healthy
        );
        // The threshold is part of the record, and a heartbeat writes it.
        let back = LapRecord::from_json(&r.to_json()).unwrap();
        assert_eq!(back.stall_seconds, Some(LISTEN_STALL_SECONDS));
        let _turn = RECORDING.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(LAPS_FILE_NAME);
        let mut hb = Heartbeat::at(Some(path.clone()), Some(LISTEN_INTERVAL_SECONDS), "sync");
        hb.set_stall_seconds(Some(LISTEN_STALL_SECONDS));
        hb.lap_started();
        drop(hb);
        assert_eq!(
            read_record(&path).unwrap().stall_seconds,
            Some(LISTEN_STALL_SECONDS)
        );
        // A new run starts without it (the loop sets it again when it listens).
        let hb = Heartbeat::at(Some(path.clone()), Some(1.0), "sync");
        assert_eq!(hb.record().stall_seconds, None);
    }

    #[test]
    fn a_loop_waiting_for_its_next_lap_is_not_stalled() {
        // Failing laps back off: 10 minutes until the next try.
        let mut r = record(Some(60.0));
        r.last_lap = Some(at(0));
        r.last_failure = Some(at(0));
        r.consecutive_failures = 5;
        r.next_lap = Some(at(600));
        let h = assess(Some(&r), Status::Running, None, at(500));
        assert_eq!(h.state, HealthState::Failing);
        // Past the due time by more than the threshold: stalled.
        let h = assess(Some(&r), Status::Running, None, at(600 + 181));
        assert_eq!(h.state, HealthState::Stalled);
        // A schedule waiting for its next fire (a day away) is healthy meanwhile.
        let mut daily = record(Some(3600.0));
        daily.last_lap = Some(at(0));
        daily.last_success = Some(at(0));
        daily.next_lap = Some(at(86400));
        assert_eq!(
            assess(Some(&daily), Status::Running, None, at(80000)).state,
            HealthState::Healthy
        );
        // A heartbeat records the wait at once when it is long.
        let _turn = RECORDING.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(LAPS_FILE_NAME);
        let mut hb = Heartbeat::at(Some(path.clone()), Some(1.0), "run");
        hb.lap_finished(false, "failed");
        hb.waiting(Duration::from_secs(120));
        let written = read_record(&path).unwrap();
        assert!(
            written
                .next_lap
                .is_some_and(|n| n > written.last_lap.unwrap())
        );
        // Once a lap ends after the due time, the due time no longer counts.
        let mut late = r.clone();
        late.last_lap = Some(at(700));
        late.consecutive_failures = 6;
        let h = assess(Some(&late), Status::Running, None, at(700 + 181));
        assert_eq!(h.state, HealthState::Stalled);
    }

    #[test]
    fn heartbeats_write_laps_and_debounce_successes() {
        let _turn = RECORDING.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let job_dir = dir.path().join("jobs").join("loop");
        std::fs::create_dir_all(&job_dir).unwrap();
        let path = job_dir.join(LAPS_FILE_NAME);
        {
            let mut hb = Heartbeat::at(Some(path.clone()), Some(10.0), "sync");
            assert!(hb.is_active());
            // A second heartbeat in the process leaves the record to the first.
            let nested = Heartbeat::at(Some(path.clone()), Some(1.0), "run");
            assert!(!nested.is_active());
            drop(nested);
            let written = read_record(&path).unwrap();
            assert_eq!(written.interval_seconds, Some(10.0));
            assert!(written.run_started.is_some() && written.last_lap.is_none());
            hb.lap_started();
            hb.lap_finished(true, "ok");
            let first = read_record(&path).unwrap();
            assert_eq!((first.laps, first.consecutive_failures), (1, 0));
            // Within the write interval a success waits; a failure is written at once.
            hb.lap_started();
            hb.lap_finished(true, "ok again");
            assert_eq!(read_record(&path).unwrap().laps, 1);
            hb.lap_started();
            hb.lap_finished(false, "postgresql://user:hunter2@db/x refused");
            let failed = read_record(&path).unwrap();
            assert_eq!((failed.laps, failed.consecutive_failures), (3, 1));
            assert!(!failed.message.unwrap().contains("hunter2"));
            hb.lap_started();
            hb.lap_finished(true, "recovered");
            assert_eq!(read_record(&path).unwrap().consecutive_failures, 0);
            hb.lap_started();
            hb.lap_finished(true, "pending");
        }
        // Dropping the heartbeat writes what was pending.
        let last = read_record(&path).unwrap();
        assert_eq!(last.laps, 5);
        assert_eq!(last.message.as_deref(), Some("pending"));
        // The claim was released: a new run records again, keeping the counters.
        let hb = Heartbeat::at(Some(path.clone()), Some(10.0), "sync");
        assert!(hb.is_active());
        assert_eq!(hb.record().laps, 5);
        drop(hb);
        // A deleted job's directory is not recreated by a heartbeat.
        std::fs::remove_dir_all(&job_dir).unwrap();
        let mut hb = Heartbeat::at(Some(path.clone()), Some(10.0), "sync");
        hb.lap_finished(true, "gone");
        drop(hb);
        assert!(!job_dir.exists());
        assert!(!Heartbeat::at(None, None, "run").is_active());
    }

    #[test]
    fn health_round_trips_through_metadata() {
        let mut r = record(Some(60.0));
        r.last_lap = Some(at(100));
        r.last_success = Some(at(100));
        let h = assess(Some(&r), Status::Running, None, at(160));
        let value = Value::from_json_str(&h.to_json().to_string()).unwrap();
        assert_eq!(JobHealth::from_value(&value), Some(h.clone()));
        assert_eq!(
            h.to_map().get("state").and_then(Value::as_str),
            Some("healthy")
        );
        assert_eq!(humanize_seconds(59.9), "59 s");
        assert_eq!(humanize_seconds(3.0 * 86400.0 + 5.0), "3 days");
        assert_eq!(humanize_seconds(86400.0), "1 day");
    }
}
