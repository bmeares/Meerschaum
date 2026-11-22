//! Per-pipe sync status: the latest outcome of each pipe's syncs.
//!
//! Every sync path records how a pipe's latest sync went, on the pipe's own instance: `sync
//! pipes` (the engine's flat and graph runs, the laps of `--loop` jobs), `--listen` callback
//! syncs and stream flushes, the API server's `POST /pipes/{connector}/{metric}/{location}/data`,
//! Python's `pipe.sync()` and plugins' `pipe.sync()` calls (through the worker or the native
//! plugin host). The record is one row per pipe in the internal pipe `mrsm:engine_sync_status`
//! (target `mrsm_engine_sync_status`; on SQL schema flavors in `_mrsm_internal` beside the rest of
//! the engine state, §5.8), keyed by `pipe_ref` and upserted:
//!
//! | Field | Meaning |
//! |---|---|
//! | `pipe_ref`, `connector_keys`, `metric_key`, `location_key`, `instance_keys` | the pipe |
//! | `last_attempt` | when the latest recorded sync ended (ISO-8601 UTC) |
//! | `last_success` / `last_failure` | when the latest successful / failed sync ended, or `null` |
//! | `success`, `message` | the latest sync's result (the message is shortened past 1,000 characters) |
//! | `rows_inserted`, `rows_updated`, `rows_upserted` | the latest sync's row counts |
//! | `duration_seconds` | how long the latest sync took |
//! | `consecutive_failures` | failed syncs since the last success (`0` after a success) |
//! | `source` | what ran the sync: `cli`, `job`, `engine` (a graph run), `api` (the API server), `python` |
//! | `job` | the job's name when the sync ran inside a job, else `null` |
//!
//! **Write rate.** Successes are debounced per pipe ([`DEBOUNCE`]): a success within five
//! seconds of the last written success is held and written when the window ends (so a burst of
//! `--listen` flushes costs one write every five seconds, and the last one is never lost while
//! the process runs). Failures, and the first success after a failure, are always written at
//! once. Each write reads the stored row first (one read, one upsert), so the failure count and
//! the last success stay right when several processes sync the same pipe; outcomes that queue up
//! while a write runs are merged into the next one.
//!
//! **Never in the way.** [`record`] returns at once: the writes run in the background, one task
//! per pipe at a time, so a sync never waits for its status. Each write is bounded by
//! [`RECORD_TIMEOUT`], and any error is logged at debug level. In the process that recorded
//! them, reads first write what it still holds for the pipes they read ([`settle_pipe`],
//! [`settle_instance`]), so a read right after a sync sees that sync; a process writes what it
//! holds before it exits ([`flush`]: the command line, the API server and the Python package
//! do).
//!
//! **Forgetting.** Dropping, deleting or fully clearing a pipe clears its stored status
//! ([`crate::pipe_ops::forget_engine_state`]) after [`forget`]: what this process held or queued
//! for the pipe is dropped and a write in progress is waited for, so no write brings the status
//! back. A held success written when its window ends is also skipped when the pipe no longer
//! exists on its instance (another process dropped or deleted it).
//!
//! **Source and job.** [`detect_source`] says what is syncing: the API server process
//! ([`set_process_role`] with [`SyncSource::Api`]) always records `api`; otherwise a process
//! started by a job (`MRSM_DAEMON_ID`) records `job` with the job's name, an engine graph run
//! records `engine`, and everything else records the process role (`cli` for the command line,
//! `python` for the Python package).
//!
//! **API instances.** An `api:` instance records on the server's own instance: the client's
//! [`InstanceConnector::record_sync_status`](crate::InstanceConnector::record_sync_status) posts its outcomes to `POST /engine/sync-status`
//! (and marks its data posts with [`CLIENT_RECORDS_HEADER`] so the server does not record them
//! twice), and its reads go to `GET /engine/sync-status`. The server keys the rows with its own
//! instance keys; the client re-keys them with its `api:<label>` keys ([`rekey`]).

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use meerschaum_core::{Map, Pipe, Value};
use serde::{Deserialize, Serialize};

mod recorder;
mod storage;

pub use recorder::{
    flush, forget, forget_on, has_unwritten, record, record_now, reset_debounce, settle_instance,
    settle_pipe, wait_written,
};
pub(crate) use storage::{doc_bool, doc_dt, doc_f64, doc_str, doc_u64, static_context};
pub use storage::{
    merge_and_store, read_status, read_statuses, status_doc, status_from_doc, status_pipe,
    stored_status, write_status,
};

/// The metric key of the internal pipe that holds the statuses (`mrsm:engine_sync_status`).
pub const METRIC: &str = "engine_sync_status";

/// Successes of one pipe are written at most this often (see the module docs).
pub const DEBOUNCE: Duration = Duration::from_secs(5);

/// The longest one write of a pipe's status (its read and its upsert) may take.
pub const RECORD_TIMEOUT: Duration = Duration::from_secs(3);

/// The longest a read waits for this process's own writes of the statuses it reads
/// ([`settle_pipe`], [`settle_instance`]).
pub const SETTLE_TIMEOUT: Duration = Duration::from_secs(4);

/// Messages longer than this many characters are shortened ([`truncate_message`]).
pub const MAX_MESSAGE_CHARS: usize = 1_000;

/// The request header with which an `api:` client says that it records the outcome of the sync
/// it posts itself (`POST /engine/sync-status`), so the server does not record it again.
pub const CLIENT_RECORDS_HEADER: &str = "x-mrsm-sync-status";

/// What ran a sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncSource {
    /// The command line (`mrsm sync pipes`, the shell, the web terminal).
    Cli,
    /// A background job (`MRSM_DAEMON_ID` names it).
    Job,
    /// An engine graph run (`--parents`, `--children`, a saved Pipeline, `Pipeline.sync()`).
    Engine,
    /// The API server (its sync route, actions and MCP tools run by the server process).
    Api,
    /// Python code (`pipe.sync()`).
    Python,
}

impl SyncSource {
    /// The name stored in the `source` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncSource::Cli => "cli",
            SyncSource::Job => "job",
            SyncSource::Engine => "engine",
            SyncSource::Api => "api",
            SyncSource::Python => "python",
        }
    }

    /// Parse a stored name.
    pub fn parse(s: &str) -> Option<SyncSource> {
        Some(match s {
            "cli" => SyncSource::Cli,
            "job" => SyncSource::Job,
            "engine" => SyncSource::Engine,
            "api" => SyncSource::Api,
            "python" => SyncSource::Python,
            _ => return None,
        })
    }

    fn code(self) -> u8 {
        match self {
            SyncSource::Cli => 1,
            SyncSource::Job => 2,
            SyncSource::Engine => 3,
            SyncSource::Api => 4,
            SyncSource::Python => 5,
        }
    }

    fn from_code(code: u8) -> Option<SyncSource> {
        Some(match code {
            1 => SyncSource::Cli,
            2 => SyncSource::Job,
            3 => SyncSource::Engine,
            4 => SyncSource::Api,
            5 => SyncSource::Python,
            _ => return None,
        })
    }
}

impl std::fmt::Display for SyncSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The process role (see [`set_process_role`]); `0` is unset.
static ROLE: AtomicU8 = AtomicU8::new(0);

/// Say what this process is, for the `source` of the syncs it records: [`SyncSource::Cli`] (the
/// command line's entry point), [`SyncSource::Python`] (the Python package, unless the command
/// line runs in it) or [`SyncSource::Api`] (`mrsm start api` while it serves).
pub fn set_process_role(role: SyncSource) {
    ROLE.store(role.code(), Ordering::SeqCst);
}

/// [`set_process_role`] unless a role is already set (the Python extension calls this when it
/// loads, so the command line it may run keeps `cli`).
pub fn set_default_process_role(role: SyncSource) {
    let _ = ROLE.compare_exchange(0, role.code(), Ordering::SeqCst, Ordering::SeqCst);
}

/// The process role ([`SyncSource::Cli`] when none was set).
pub fn process_role() -> SyncSource {
    SyncSource::from_code(ROLE.load(Ordering::SeqCst)).unwrap_or(SyncSource::Cli)
}

/// Sets a process role until dropped, then restores the previous one ([`role_scope`]).
#[derive(Debug)]
#[must_use = "the role is restored when the scope is dropped"]
pub struct RoleScope {
    previous: u8,
}

impl Drop for RoleScope {
    fn drop(&mut self) {
        ROLE.store(self.previous, Ordering::SeqCst);
    }
}

/// Set the process role for as long as the returned scope lives (the API server while it
/// serves).
pub fn role_scope(role: SyncSource) -> RoleScope {
    RoleScope {
        previous: ROLE.swap(role.code(), Ordering::SeqCst),
    }
}

/// The name of the job this process runs in (`MRSM_DAEMON_ID`), if any.
pub fn current_job() -> Option<String> {
    let var = &meerschaum_core::static_config::static_config()
        .environment
        .daemon_id;
    std::env::var(var)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// The `source` and `job` of a sync this process runs now (`graph_run`: an engine graph run).
/// See the module docs for the order.
pub fn detect_source(graph_run: bool) -> (SyncSource, Option<String>) {
    let role = process_role();
    if role == SyncSource::Api {
        return (SyncSource::Api, None);
    }
    if let Some(job) = current_job() {
        return (SyncSource::Job, Some(job));
    }
    if graph_run {
        return (SyncSource::Engine, None);
    }
    (role, None)
}

/// The outcome of one sync, merged into the pipe's [`SyncStatus`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncOutcome {
    /// Registration/ACL segment captured before primary work; internal transport evidence.
    #[serde(default, skip_serializing)]
    pub registration_id: Option<String>,
    /// Distinguish an intentionally unproven capture from a trusted local snapshot fallback.
    #[serde(skip)]
    pub registration_captured: bool,
    /// Whether the sync succeeded.
    pub success: bool,
    /// Its message (or error).
    pub message: String,
    /// Rows inserted.
    #[serde(default)]
    pub inserted: u64,
    /// Rows updated.
    #[serde(default)]
    pub updated: u64,
    /// Rows upserted.
    #[serde(default)]
    pub upserted: u64,
    /// How long it took, in seconds.
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    /// The engine run, when this outcome belongs to one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Attempts made, including the first attempt.
    #[serde(default = "one_attempt")]
    pub attempts: u32,
    /// Chunks completed.
    #[serde(default)]
    pub chunks: u64,
    /// Rows deleted by a replacement sync.
    #[serde(default)]
    pub deleted: u64,
    /// When it ended.
    #[serde(with = "iso")]
    pub finished: DateTime<Utc>,
    /// What ran it.
    pub source: SyncSource,
    /// The job it ran in.
    #[serde(default)]
    pub job: Option<String>,
}

impl SyncOutcome {
    /// An outcome that ended now.
    pub fn new(
        success: bool,
        message: impl Into<String>,
        source: SyncSource,
        job: Option<String>,
    ) -> SyncOutcome {
        SyncOutcome {
            registration_id: None,
            registration_captured: false,
            success,
            message: message.into(),
            inserted: 0,
            updated: 0,
            upserted: 0,
            duration_seconds: None,
            run_id: None,
            attempts: 1,
            chunks: 1,
            deleted: 0,
            finished: Utc::now(),
            source,
            job,
        }
    }

    /// Attach the immutable registration segment used by this operation (including no proof).
    pub fn with_registration(mut self, registration_id: Option<String>) -> Self {
        self.registration_id = registration_id;
        self.registration_captured = true;
        self
    }

    /// An outcome that ended now, with the source and job of this process
    /// ([`detect_source`]).
    pub fn detected(success: bool, message: impl Into<String>, graph_run: bool) -> SyncOutcome {
        let (source, job) = detect_source(graph_run);
        SyncOutcome::new(success, message, source, job)
    }

    /// With row counts.
    pub fn with_counts(mut self, inserted: u64, updated: u64, upserted: u64) -> SyncOutcome {
        self.inserted = inserted;
        self.updated = updated;
        self.upserted = upserted;
        self
    }

    /// With the counts written in a sync message (`Inserted 10, updated 2 rows.`,
    /// `Upserted 5 rows.`), for results that carry no counts of their own.
    pub fn with_message_counts(mut self) -> SyncOutcome {
        let (inserted, updated, upserted) = message_counts(&self.message);
        self.deleted =
            crate::pipe_ops::sync_result_from_message(self.success, self.message.clone()).deleted;
        self.with_counts(inserted, updated, upserted)
    }

    /// With a duration.
    pub fn with_duration(mut self, duration: Option<Duration>) -> SyncOutcome {
        self.duration_seconds = duration.map(|d| d.as_secs_f64());
        self
    }
}

fn one_attempt() -> u32 {
    1
}

/// The row counts a successful sync message reports: `(inserted, updated, upserted)`
/// ([`crate::pipe_ops::sync_result_from_message`]).
pub fn message_counts(message: &str) -> (u64, u64, u64) {
    let r = crate::pipe_ops::sync_result_from_message(true, message.to_string());
    (r.inserted, r.updated, r.upserted)
}

/// One pipe's sync status (the C2 record).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncStatus {
    /// Storage-only generation; never expose internal registration evidence in public JSON.
    #[serde(default, skip_serializing)]
    pub registration_id: Option<String>,
    /// `"{instance_keys}|{connector_keys}|{metric_key}|{location_key}"`.
    pub pipe_ref: String,
    /// The connector keys.
    pub connector_keys: String,
    /// The metric key.
    pub metric_key: String,
    /// The location key.
    pub location_key: Option<String>,
    /// The instance keys.
    pub instance_keys: String,
    /// When the latest recorded sync ended.
    #[serde(with = "iso_opt", default)]
    pub last_attempt: Option<DateTime<Utc>>,
    /// When the latest successful sync ended.
    #[serde(with = "iso_opt", default)]
    pub last_success: Option<DateTime<Utc>>,
    /// When the latest failed sync ended.
    #[serde(with = "iso_opt", default)]
    pub last_failure: Option<DateTime<Utc>>,
    /// Whether the latest sync succeeded.
    pub success: bool,
    /// The latest sync's message (shortened, [`truncate_message`]).
    #[serde(default)]
    pub message: String,
    /// Rows the latest sync inserted.
    #[serde(default)]
    pub rows_inserted: u64,
    /// Rows the latest sync updated.
    #[serde(default)]
    pub rows_updated: u64,
    /// Rows the latest sync upserted (upsert pipes cannot tell inserts from updates).
    #[serde(default)]
    pub rows_upserted: u64,
    /// How long the latest sync took, in seconds.
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    /// Failed syncs since the last success.
    #[serde(default)]
    pub consecutive_failures: u64,
    /// What ran the latest sync (`cli`, `job`, `engine`, `api`, `python`).
    #[serde(default)]
    pub source: String,
    /// The job the latest sync ran in.
    #[serde(default)]
    pub job: Option<String>,
}

impl SyncStatus {
    /// An empty status for `pipe` (nothing recorded yet).
    pub fn for_pipe(pipe: &Pipe) -> SyncStatus {
        SyncStatus {
            registration_id: crate::sync_metrics::observed_registration(pipe),
            pipe_ref: pipe.pipe_ref(),
            connector_keys: pipe.connector_keys().to_string(),
            metric_key: pipe.metric_key().to_string(),
            location_key: pipe.location_key().map(str::to_string),
            instance_keys: pipe.instance_keys().to_string(),
            last_attempt: None,
            last_success: None,
            last_failure: None,
            success: false,
            message: String::new(),
            rows_inserted: 0,
            rows_updated: 0,
            rows_upserted: 0,
            duration_seconds: None,
            consecutive_failures: 0,
            source: String::new(),
            job: None,
        }
    }

    /// Merge one outcome. An outcome older than the latest recorded attempt (a held success
    /// written late, another process's slower sync) only moves `last_success` / `last_failure`
    /// forward; the latest attempt's fields stay.
    pub fn apply(mut self, outcome: &SyncOutcome) -> SyncStatus {
        let at = outcome.finished;
        let later = |t: Option<DateTime<Utc>>| Some(t.map_or(at, |t| t.max(at)));
        if self.last_attempt.is_some_and(|t| at < t) {
            if outcome.success {
                self.last_success = later(self.last_success);
            } else {
                self.last_failure = later(self.last_failure);
            }
            return self;
        }
        self.last_attempt = Some(at);
        self.success = outcome.success;
        self.message = truncate_message(&outcome.message);
        self.rows_inserted = outcome.inserted;
        self.rows_updated = outcome.updated;
        self.rows_upserted = outcome.upserted;
        self.duration_seconds = outcome.duration_seconds;
        self.source = outcome.source.as_str().to_string();
        self.job = outcome.job.clone();
        if outcome.success {
            self.last_success = Some(at);
            self.consecutive_failures = 0;
        } else {
            self.last_failure = Some(at);
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        }
        self
    }

    /// The JSON object of the C2 contract.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// `status` keyed under `instance_keys` (an `api:` client's keys for a row its server keeps, or
/// the keys a status was read under).
pub fn rekey(mut status: SyncStatus, instance_keys: &str) -> SyncStatus {
    status.instance_keys = instance_keys.to_string();
    status.pipe_ref = format!(
        "{instance_keys}|{}|{}|{}",
        status.connector_keys,
        status.metric_key,
        status.location_key.as_deref().unwrap_or("None")
    );
    status
}

/// `message` trimmed, and shortened to about [`MAX_MESSAGE_CHARS`] characters when longer: the
/// beginning (what failed) and the end (a traceback's last lines) are kept around a `…` line.
pub fn truncate_message(message: &str) -> String {
    let message = message.trim();
    let count = message.chars().count();
    if count <= MAX_MESSAGE_CHARS {
        return message.to_string();
    }
    let head_chars = MAX_MESSAGE_CHARS * 7 / 10;
    let tail_chars = MAX_MESSAGE_CHARS / 4;
    let head: String = message.chars().take(head_chars).collect();
    let tail: String = message.chars().skip(count - tail_chars).collect();
    format!("{}\n…\n{}", head.trim_end(), tail.trim_start())
}

/// ISO-8601 UTC text (`2026-10-27T12:00:00.000000+00:00`, which Python's
/// `datetime.fromisoformat` reads).
pub fn format_iso(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

/// Parse ISO-8601 text (any offset, or none for UTC).
pub fn parse_iso(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s.trim()) {
        return Some(dt.with_timezone(&Utc));
    }
    meerschaum_kernel::value::parse_datetime_value(s.trim())
        .map(|d| DateTime::<Utc>::from_naive_utc_and_offset(d.utc, Utc))
}

mod iso {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::format_iso(dt))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        super::parse_iso(&s).ok_or_else(|| serde::de::Error::custom(format!("invalid time '{s}'")))
    }
}

mod iso_opt {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(dt: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
        match dt {
            Some(dt) => s.serialize_str(&super::format_iso(dt)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<DateTime<Utc>>, D::Error> {
        let s: Option<String> = Option::deserialize(d)?;
        Ok(s.as_deref().and_then(super::parse_iso))
    }
}

/// Whether syncs of `pipe` have a status: not the internal `mrsm` pipes and not `temporary`
/// pipes (they are never registered, so nothing lists them).
pub fn is_recorded(pipe: &Pipe) -> bool {
    !pipe.temporary && pipe.connector_keys() != crate::policy::INTERNAL_CONNECTOR_KEYS
}

tokio::task_local! {
    /// Set while a sync runs whose outcome its sync path records ([`recorded_scope`]).
    static RECORDED_BY_SYNC_PATH: ();
}

struct RegistrationCapture {
    reference: String,
    identity: Option<String>,
}

tokio::task_local! {
    static CAPTURED_REGISTRATION: Arc<Mutex<RegistrationCapture>>;
}

/// Capture the actual immutable registry metadata loaded by a bare sync, before primary work.
pub async fn registration_scope<F: std::future::Future>(
    pipe: &Pipe,
    future: F,
) -> (F::Output, Option<String>) {
    let capture = Arc::new(Mutex::new(RegistrationCapture {
        reference: pipe.pipe_ref(),
        identity: None,
    }));
    let result = CAPTURED_REGISTRATION.scope(capture.clone(), future).await;
    let identity = capture
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .identity
        .clone();
    (result, identity)
}

/// Supply a pre-work registry snapshot. The first proven segment cannot be retagged later.
pub fn capture_registration(pipe: &Pipe, identity: Option<String>) {
    let _ = CAPTURED_REGISTRATION.try_with(|capture| {
        let mut capture = capture.lock().unwrap_or_else(|error| error.into_inner());
        if capture.reference == pipe.pipe_ref() && capture.identity.is_none() {
            capture.identity = identity;
        }
    });
}

/// A single atomic registration row's segment; malformed/ID-less metadata is unproven.
pub fn registration_from_attributes(attributes: &Map) -> Option<String> {
    let identity = attributes.get("pipe_id")?;
    if !matches!(identity, Value::Int(_) | Value::Str(_)) {
        return None;
    }
    let parameters = match attributes.get("parameters")? {
        Value::Map(parameters) => parameters.clone(),
        Value::Str(text) => match Value::from_json_str(text).ok()? {
            Value::Map(parameters) => parameters,
            _ => return None,
        },
        _ => return None,
    };
    Some(crate::sync_metrics::registration_stamp(
        &identity.py_str(),
        &parameters,
    ))
}

/// Run `fut` (a sync) as one whose outcome the caller records: an `api:` client then marks the
/// data it posts with [`CLIENT_RECORDS_HEADER`], so its server does not record the posts too.
/// A sync without this scope (a direct `sync_pipe` of a copy) is recorded by the server.
pub async fn recorded_scope<F: std::future::Future>(fut: F) -> F::Output {
    RECORDED_BY_SYNC_PATH.scope((), fut).await
}

/// Whether the current task runs inside [`recorded_scope`].
pub fn in_recorded_sync() -> bool {
    RECORDED_BY_SYNC_PATH.try_with(|_| ()).is_ok()
}

#[cfg(test)]
mod tests;
