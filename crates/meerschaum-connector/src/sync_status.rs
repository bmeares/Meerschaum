//! Per-pipe sync status (rc15 contract C2): the latest outcome of each pipe's syncs.
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
//! [`InstanceConnector::record_sync_status`] posts its outcomes to `POST /engine/sync-status`
//! (and marks its data posts with [`CLIENT_RECORDS_HEADER`] so the server does not record them
//! twice), and its reads go to `GET /engine/sync-status`. The server keys the rows with its own
//! instance keys; the client re-keys them with its `api:<label>` keys ([`rekey`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use serde::{Deserialize, Serialize};

use crate::pipe_ops::{Doc, frame_to_docs};
use crate::traits::{ConnectorError, ConnectorResult, DataQuery, InstanceConnector, SyncOptions};

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

// ---------------------------------------------------------------------------------------------
// Storage: the `mrsm:engine_sync_status` pipe on the instance.

/// The status pipe on `conn`, registered as `instance_keys`
/// ([`InstanceConnector::engine_state_pipe`]).
pub async fn status_pipe<C: InstanceConnector + ?Sized>(
    conn: &C,
    instance_keys: &str,
) -> ConnectorResult<Pipe> {
    conn.engine_state_pipe(instance_keys, METRIC)
        .await?
        .or_else(|| crate::pipe_ops::engine_state_pipe(instance_keys, METRIC))
        .ok_or_else(|| ConnectorError::Invalid(format!("'{METRIC}' is not an engine state pipe.")))
}

fn kv_str(s: &str) -> KValue {
    KValue::Str(s.to_string())
}

fn kv_opt_str(s: Option<&str>) -> KValue {
    s.map(kv_str).unwrap_or(KValue::Null)
}

fn kv_opt_dt(dt: Option<DateTime<Utc>>) -> KValue {
    dt.map(|dt| KValue::Datetime(DateTimeValue::utc(dt.naive_utc())))
        .unwrap_or(KValue::Null)
}

fn kv_int(n: u64) -> KValue {
    KValue::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

/// A status as a row of the status pipe.
pub fn status_doc(s: &SyncStatus) -> Doc {
    [
        ("registration_id", kv_opt_str(s.registration_id.as_deref())),
        ("pipe_ref", kv_str(&s.pipe_ref)),
        ("connector_keys", kv_str(&s.connector_keys)),
        ("metric_key", kv_str(&s.metric_key)),
        ("location_key", kv_opt_str(s.location_key.as_deref())),
        ("instance_keys", kv_str(&s.instance_keys)),
        ("last_attempt", kv_opt_dt(s.last_attempt)),
        ("last_success", kv_opt_dt(s.last_success)),
        ("last_failure", kv_opt_dt(s.last_failure)),
        ("success", KValue::Bool(s.success)),
        ("message", kv_str(&s.message)),
        ("rows_inserted", kv_int(s.rows_inserted)),
        ("rows_updated", kv_int(s.rows_updated)),
        ("rows_upserted", kv_int(s.rows_upserted)),
        (
            "duration_seconds",
            s.duration_seconds
                .map(KValue::Float)
                .unwrap_or(KValue::Null),
        ),
        ("consecutive_failures", kv_int(s.consecutive_failures)),
        ("source", kv_str(&s.source)),
        ("job", kv_opt_str(s.job.as_deref())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

pub(crate) fn doc_str(doc: &Doc, key: &str) -> Option<String> {
    match doc.get(key)? {
        v if v.is_na() => None,
        KValue::Str(s) => Some(s.clone()),
        other => Some(other.py_str()),
    }
}

pub(crate) fn doc_u64(doc: &Doc, key: &str) -> u64 {
    let n = match doc.get(key) {
        Some(KValue::Int(i)) => *i,
        Some(KValue::Float(f)) if f.is_finite() => *f as i64,
        Some(KValue::Bool(b)) => *b as i64,
        Some(KValue::Str(s)) => s.trim().parse().unwrap_or(0),
        Some(KValue::Decimal(d)) => d.to_f64() as i64,
        _ => 0,
    };
    n.max(0) as u64
}

pub(crate) fn doc_f64(doc: &Doc, key: &str) -> Option<f64> {
    match doc.get(key)? {
        KValue::Float(f) if f.is_finite() => Some(*f),
        KValue::Int(i) => Some(*i as f64),
        KValue::Str(s) => s.trim().parse().ok(),
        KValue::Decimal(d) => Some(d.to_f64()),
        _ => None,
    }
}

pub(crate) fn doc_dt(doc: &Doc, key: &str) -> Option<DateTime<Utc>> {
    match doc.get(key)? {
        KValue::Datetime(d) => Some(DateTime::from_naive_utc_and_offset(d.utc, Utc)),
        KValue::Str(s) => parse_iso(s),
        _ => None,
    }
}

pub(crate) fn doc_bool(doc: &Doc, key: &str) -> bool {
    match doc.get(key) {
        Some(KValue::Bool(b)) => *b,
        Some(KValue::Int(i)) => *i != 0,
        Some(KValue::Float(value)) => value.is_finite() && *value != 0.0,
        Some(KValue::Str(s)) => matches!(s.trim(), "True" | "true" | "1"),
        _ => false,
    }
}

/// A row of the status pipe as a status (`None` without the pipe's keys).
pub fn status_from_doc(doc: &Doc) -> Option<SyncStatus> {
    let connector_keys = doc_str(doc, "connector_keys")?;
    let metric_key = doc_str(doc, "metric_key")?;
    let location_key = doc_str(doc, "location_key").filter(|l| l != "None");
    let instance_keys = doc_str(doc, "instance_keys").unwrap_or_default();
    let pipe_ref = doc_str(doc, "pipe_ref").unwrap_or_else(|| {
        format!(
            "{instance_keys}|{connector_keys}|{metric_key}|{}",
            location_key.as_deref().unwrap_or("None")
        )
    });
    Some(SyncStatus {
        registration_id: doc_str(doc, "registration_id"),
        pipe_ref,
        connector_keys,
        metric_key,
        location_key,
        instance_keys,
        last_attempt: doc_dt(doc, "last_attempt"),
        last_success: doc_dt(doc, "last_success"),
        last_failure: doc_dt(doc, "last_failure"),
        success: doc_bool(doc, "success"),
        message: doc_str(doc, "message").unwrap_or_default(),
        rows_inserted: doc_u64(doc, "rows_inserted"),
        rows_updated: doc_u64(doc, "rows_updated"),
        rows_upserted: doc_u64(doc, "rows_upserted"),
        duration_seconds: doc_f64(doc, "duration_seconds"),
        consecutive_failures: doc_u64(doc, "consecutive_failures"),
        source: doc_str(doc, "source").unwrap_or_default(),
        job: doc_str(doc, "job"),
    })
}

/// The declared dtypes of the status pipe as an enforcement context (no instance query).
pub(crate) fn static_context(pipe: &Pipe) -> PipeDtypesContext {
    let dtypes: IndexMap<String, String> = match pipe.parameters().get("dtypes") {
        Some(Value::Map(m)) => m
            .iter()
            .map(|(k, v)| (k.clone(), meerschaum_core::pyfmt::py_str(v)))
            .collect(),
        _ => IndexMap::new(),
    };
    PipeDtypesContext {
        explicit_dtypes: dtypes.clone(),
        dtypes,
        enforce: true,
        tz_aware: true,
        mixed_numerics: true,
    }
}

/// Of several stored rows for one pipe (written under other labels of the same instance), the
/// latest.
fn latest(statuses: impl IntoIterator<Item = SyncStatus>) -> Option<SyncStatus> {
    statuses.into_iter().max_by_key(|s| s.last_attempt)
}

/// The stored statuses on `conn` (any `params` filter), re-keyed under `instance_keys`: the
/// table belongs to the instance, whatever label wrote a row. `None` for a missing table.
async fn read_rows<C: InstanceConnector + ?Sized>(
    conn: &C,
    instance_keys: &str,
    params: Option<Map>,
) -> ConnectorResult<Vec<SyncStatus>> {
    let pipe = status_pipe(conn, instance_keys).await?;
    let query = DataQuery {
        params,
        ..Default::default()
    };
    let df = match conn.get_pipe_data(&pipe, &query).await? {
        Some(df) => df,
        None => return Ok(Vec::new()),
    };
    Ok(frame_to_docs(&df)
        .iter()
        .filter_map(status_from_doc)
        .map(|s| rekey(s, instance_keys))
        .collect())
}

/// The status of `pipe` on `conn`, after writing what this process holds for it
/// ([`settle_pipe`]; the default of [`InstanceConnector::get_sync_status`]).
pub async fn read_status<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<Option<SyncStatus>> {
    settle_pipe(&pipe.pipe_ref()).await;
    stored_status(conn, pipe).await
}

/// The stored status of `pipe` on `conn`, without waiting for this process's writes (what a
/// write merges its outcomes into).
pub async fn stored_status<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<Option<SyncStatus>> {
    let mut params = Map::new();
    params.insert(
        "connector_keys".into(),
        Value::Str(pipe.connector_keys().into()),
    );
    params.insert("metric_key".into(), Value::Str(pipe.metric_key().into()));
    let rows = read_rows(conn, pipe.instance_keys(), Some(params)).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let attributes = conn.get_pipe_attributes(pipe).await?;
    let current = registration_from_attributes(&attributes);
    Ok(latest(rows.into_iter().filter(|s| {
        s.location_key.as_deref() == pipe.location_key()
            && if attributes.is_empty() {
                s.registration_id.is_none()
            } else {
                current.is_some() && s.registration_id == current
            }
    })))
}

/// Every stored status on `conn`, keyed under `instance_keys`, one per pipe, in key order, after
/// writing what this process holds for that instance ([`settle_instance`]; the default of
/// [`InstanceConnector::get_sync_statuses`]).
pub async fn read_statuses<C: InstanceConnector + ?Sized>(
    conn: &C,
    instance_keys: &str,
) -> ConnectorResult<Vec<SyncStatus>> {
    settle_instance(instance_keys).await;
    let rows = read_rows(conn, instance_keys, None).await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let registrations = conn
        .registered_pipes(&crate::PipesKeysQuery::default())
        .await?;
    let mut current: HashMap<String, Option<String>> = HashMap::new();
    for (identity, entry) in registrations {
        let key = crate::sync_metrics::pipe_key(
            &entry.connector_keys,
            &entry.metric_key,
            entry.location_key.as_deref(),
        );
        let stamp = identity.and_then(|identity| match entry.extra {
            Some(Value::Map(parameters)) => Some(crate::sync_metrics::registration_stamp(
                &identity.to_string(),
                &parameters,
            )),
            Some(Value::Str(text)) => match Value::from_json_str(&text).ok()? {
                Value::Map(parameters) => Some(crate::sync_metrics::registration_stamp(
                    &identity.to_string(),
                    &parameters,
                )),
                _ => None,
            },
            _ => None,
        });
        current.insert(key, stamp);
    }
    let mut by_ref: IndexMap<String, SyncStatus> = IndexMap::new();
    for status in rows {
        let key = crate::sync_metrics::pipe_key(
            &status.connector_keys,
            &status.metric_key,
            status.location_key.as_deref(),
        );
        let visible = match current.get(&key) {
            Some(Some(stamp)) => status.registration_id.as_ref() == Some(stamp),
            Some(None) => false,
            None => status.registration_id.is_none(),
        };
        if !visible {
            continue;
        }
        match by_ref.get(&status.pipe_ref) {
            Some(existing) if existing.last_attempt >= status.last_attempt => {}
            _ => {
                by_ref.insert(status.pipe_ref.clone(), status);
            }
        }
    }
    let mut out: Vec<SyncStatus> = by_ref.into_values().collect();
    out.sort_by(|a, b| a.pipe_ref.cmp(&b.pipe_ref));
    Ok(out)
}

/// Upsert `status` into the status pipe on `conn`.
pub async fn write_status<C: InstanceConnector + ?Sized>(
    conn: &C,
    status: &SyncStatus,
) -> ConnectorResult<()> {
    let pipe = status_pipe(conn, &status.instance_keys).await?;
    let df = pipe_enforce_dtypes(
        PipeInput::Docs(vec![status_doc(status)]),
        &static_context(&pipe),
    )
    .map_err(|e| ConnectorError::Invalid(format!("cannot build the sync status row: {e}")))?;
    let mut extra = Map::new();
    extra.insert("_check_temporary_tables".into(), Value::Bool(false));
    extra.insert("enforce".into(), Value::Bool(false));
    let res = conn
        .sync_pipe(
            &pipe,
            df,
            &SyncOptions {
                check_existing: true,
                extra,
                ..Default::default()
            },
        )
        .await;
    if res.success {
        Ok(())
    } else {
        Err(ConnectorError::Invalid(format!(
            "cannot write the sync status of {}: {}",
            status.pipe_ref, res.message
        )))
    }
}

/// Merge `outcomes` (oldest first) into the stored status of `pipe` on `conn` and store it (the
/// default of [`InstanceConnector::record_sync_status`]): one read, one upsert.
pub async fn merge_and_store<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    outcomes: &[SyncOutcome],
) -> ConnectorResult<Option<SyncStatus>> {
    if outcomes.is_empty() {
        return stored_status(conn, pipe).await;
    }
    let previous = stored_status(conn, pipe).await?;
    let base = match previous {
        // Keep the stored history under this call's keys.
        Some(s) => rekey(s, pipe.instance_keys()),
        None => SyncStatus::for_pipe(pipe),
    };
    let mut status = base;
    for outcome in outcomes {
        let captured = if outcome.registration_captured {
            outcome.registration_id.clone()
        } else {
            crate::sync_metrics::observed_registration(pipe)
        };
        if status.registration_id != captured {
            status = SyncStatus::for_pipe(pipe);
        }
        status.registration_id = captured;
        status = status.apply(outcome);
    }
    let current = conn.get_pipe_attributes(pipe).await?;
    let matches = if current.is_empty() {
        status.registration_id.is_none()
    } else {
        registration_from_attributes(&current)
            .is_some_and(|identity| status.registration_id.as_ref() == Some(&identity))
    };
    if !matches {
        return Ok(None);
    }
    write_status(conn, &status).await?;
    let attributes = conn.get_pipe_attributes(pipe).await?;
    let proven = if attributes.is_empty() {
        status.registration_id.is_none()
    } else {
        registration_from_attributes(&attributes)
            .is_some_and(|current| status.registration_id.as_ref() == Some(&current))
    };
    Ok(proven.then_some(status))
}

// ---------------------------------------------------------------------------------------------
// Recording: debounce, background writes, time bound, never failing.

/// The recording state of one pipe in this process.
#[derive(Default)]
struct Track {
    /// A privacy/registration change starts a fresh debounce segment too.
    registration_id: Option<String>,
    /// When its status was last written (or a write was decided).
    last_write: Option<Instant>,
    /// Whether the last written outcome was a success.
    last_success: bool,
    /// A success held back by the debounce.
    pending: Option<SyncOutcome>,
    /// A timer will queue `pending` when the window ends.
    flush_scheduled: bool,
    /// Outcomes to write, oldest first: a writer merges them in one read and one upsert.
    queue: Vec<SyncOutcome>,
    /// A writer task writes `queue` (until it is empty).
    writing: bool,
    /// Where the outcomes go: the instance connector and the pipe of the latest outcome.
    /// Held strongly only while an outcome still needs to be written.
    target: Option<(Arc<dyn InstanceConnector>, Pipe)>,
    /// Preserve debounce state while the caller still owns this connector.
    owner: Option<Weak<dyn InstanceConnector>>,
    /// Bumped when the pipe's state is forgotten ([`forget`]): nothing decided before is written.
    epoch: u64,
}

impl Track {
    fn take_idle_target(&mut self) -> Option<(Arc<dyn InstanceConnector>, Pipe)> {
        if self.writing || self.pending.is_some() || !self.queue.is_empty() {
            return None;
        }
        self.target.take()
    }
}

/// What [`decide`] says to do with an outcome.
#[derive(Debug, PartialEq)]
enum Decision {
    /// Write these outcomes now (a held success first).
    Write(Vec<SyncOutcome>),
    /// Held back; a flush is due at this instant (when one needs scheduling).
    Hold { schedule: Option<Instant> },
}

/// One connector owns its own queue even when another connector uses the same visible alias.
#[derive(Clone, PartialEq, Eq, Hash)]
struct TrackKey {
    pipe_ref: String,
    owner: usize,
}

impl TrackKey {
    fn new<C: InstanceConnector + ?Sized>(conn: &C, pipe_ref: &str) -> Self {
        Self {
            pipe_ref: pipe_ref.into(),
            owner: conn as *const C as *const () as usize,
        }
    }
}

/// This process's recording state, by connector and `pipe_ref`. A forked child (v4-style job daemons fork)
/// starts afresh: its parent's held successes, queued outcomes, writers and locks are not its
/// own.
#[derive(Default)]
struct State {
    pid: u32,
    tracks: HashMap<TrackKey, Track>,
    locks: HashMap<String, Arc<tokio::sync::Mutex<()>>>,
    next_epoch: u64,
    last_prune: Option<Instant>,
}

impl State {
    fn new_epoch(&mut self) -> u64 {
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.next_epoch
    }

    fn prune_idle(&mut self) {
        let now = Instant::now();
        if self
            .last_prune
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
        {
            return;
        }
        self.last_prune = Some(now);
        self.tracks.retain(|_, track| {
            track.writing
                || track.pending.is_some()
                || !track.queue.is_empty()
                || track.target.is_some()
                || track
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.strong_count() > 0)
        });
        self.locks.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
}

/// The recording state of this process (reset when the process ID changed: a fork).
fn state() -> std::sync::MutexGuard<'static, State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    let mut guard = STATE
        .get_or_init(|| Mutex::new(State::default()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let pid = std::process::id();
    if guard.pid != pid {
        *guard = State {
            pid,
            ..State::default()
        };
    }
    guard
}

/// Woken whenever a writer finished a batch or stopped, and when a pipe is forgotten (what
/// [`settle_where`] waits on).
fn changed() -> &'static tokio::sync::Notify {
    static CHANGED: OnceLock<tokio::sync::Notify> = OnceLock::new();
    CHANGED.get_or_init(tokio::sync::Notify::new)
}

fn write_lock(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    state().locks.entry(key.to_string()).or_default().clone()
}

/// The debounce rule for one outcome of the pipe `key` at `now` (see the module docs).
fn decide(track: &mut Track, outcome: SyncOutcome, now: Instant, window: Duration) -> Decision {
    let within = track
        .last_write
        .is_some_and(|w| now.saturating_duration_since(w) < window);
    if outcome.success && track.last_success && within {
        track.pending = Some(outcome);
        let schedule = (!track.flush_scheduled).then(|| {
            track.flush_scheduled = true;
            track.last_write.map_or(now, |w| w + window)
        });
        return Decision::Hold { schedule };
    }
    let mut batch: Vec<SyncOutcome> = track.pending.take().into_iter().collect();
    track.last_success = outcome.success;
    track.last_write = Some(now);
    batch.push(outcome);
    Decision::Write(batch)
}

/// Mark `track` as written by a new writer task, unless one runs already or nothing is queued.
fn claim_writer(track: &mut Track) -> bool {
    if track.writing || track.queue.is_empty() {
        return false;
    }
    track.writing = true;
    true
}

/// Forget this process's debounce state of one pipe (tests: the next success writes at once).
#[doc(hidden)]
pub fn reset_debounce(pipe_ref: &str) {
    for (key, track) in &mut state().tracks {
        if key.pipe_ref != pipe_ref {
            continue;
        }
        track.last_write = None;
        track.last_success = false;
    }
}

/// Record `outcome` for `pipe` on its instance `conn` (see the module docs): debounced and
/// written in the background, so this returns at once and never fails. Pipes without a status
/// ([`is_recorded`]) are skipped.
pub fn record(conn: Arc<dyn InstanceConnector>, pipe: &Pipe, outcome: SyncOutcome) {
    record_in(conn, pipe, outcome, DEBOUNCE);
}

/// [`record`] with a debounce `window`.
fn record_in(
    conn: Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    mut outcome: SyncOutcome,
    window: Duration,
) {
    if !is_recorded(pipe) {
        return;
    }
    if !outcome.registration_captured {
        outcome = outcome.with_registration(crate::sync_metrics::observed_registration(pipe));
    }
    crate::sync_metrics::record(conn.clone(), pipe, &outcome);
    let key = TrackKey::new(conn.as_ref(), &pipe.pipe_ref());
    let (start, timer, _replaced) = {
        let mut state = state();
        state.prune_idle();
        // An old timer must not act on a new connector that reuses a freed address.
        let epoch = state.new_epoch();
        let track = state.tracks.entry(key.clone()).or_insert_with(|| Track {
            epoch,
            ..Track::default()
        });
        track.owner = Some(Arc::downgrade(&conn));
        if track.registration_id != outcome.registration_id {
            track.registration_id = outcome.registration_id.clone();
            track.last_write = None;
            track.last_success = false;
            track.pending = None;
        }
        // The previous target is dropped after the lock (it may hold the last connector).
        let replaced = track.target.replace((conn, pipe.clone()));
        match decide(track, outcome, Instant::now(), window) {
            Decision::Write(outcomes) => {
                track.queue.extend(outcomes);
                (claim_writer(track), None, replaced)
            }
            Decision::Hold { schedule } => {
                (false, schedule.map(|due| (due, track.epoch)), replaced)
            }
        }
    };
    if start {
        // Without a runtime the outcomes wait for the next record or read on one.
        drop(start_writer(key.clone()));
    }
    if let Some((due, epoch)) = timer {
        start_timer(key, due, epoch);
    }
}

/// Clears a pipe's `writing` mark when its writer ends without doing so itself (its runtime
/// shut down, or it never ran), so the next record or read starts another.
struct WriterGuard {
    key: TrackKey,
    armed: bool,
}

impl Drop for WriterGuard {
    fn drop(&mut self) {
        if self.armed {
            let retired = {
                let mut state = state();
                state.tracks.get_mut(&self.key).and_then(|track| {
                    track.writing = false;
                    track.take_idle_target()
                })
            };
            drop(retired);
            changed().notify_waiters();
        }
    }
}

/// Start the writer of the pipe `key` (claimed with [`claim_writer`]) on the current runtime.
/// Without a runtime, the writer is returned for the caller to await (or drop).
fn start_writer(key: TrackKey) -> Option<impl std::future::Future<Output = ()> + Send + 'static> {
    let guard = WriterGuard {
        key: key.clone(),
        armed: true,
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(write_queue(key, guard));
            None
        }
        Err(_) => Some(write_queue(key, guard)),
    }
}

/// Write the queued outcomes of the pipe `key` until none are left.
async fn write_queue(key: TrackKey, mut guard: WriterGuard) {
    loop {
        let batch = {
            let mut state = state();
            let Some(track) = state.tracks.get_mut(&key) else {
                guard.armed = false;
                return;
            };
            if track.queue.is_empty() {
                track.writing = false;
                let retired = track.take_idle_target();
                guard.armed = false;
                drop(state);
                drop(retired);
                changed().notify_waiters();
                return;
            }
            let outcomes = std::mem::take(&mut track.queue);
            track
                .target
                .clone()
                .map(|(conn, pipe)| (conn, pipe, outcomes, track.epoch))
        };
        if let Some((conn, pipe, outcomes, epoch)) = batch {
            write_batch(&key, conn.as_ref(), &pipe, &outcomes, epoch).await;
        }
        changed().notify_waiters();
    }
}

/// Merge `outcomes` into the stored status of `pipe` under its write lock, unless the pipe was
/// forgotten since they were queued (`epoch`): bounded by [`RECORD_TIMEOUT`], errors logged at
/// debug level.
async fn write_batch(
    key: &TrackKey,
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    outcomes: &[SyncOutcome],
    epoch: u64,
) {
    let lock = write_lock(&key.pipe_ref);
    let write = async {
        let _guard = lock.lock().await;
        if state().tracks.get(key).map(|t| t.epoch) != Some(epoch) {
            return Ok(None);
        }
        conn.record_sync_status(pipe, outcomes).await
    };
    match tokio::time::timeout(RECORD_TIMEOUT, write).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::debug!("Cannot record the sync status of {pipe}: {e}"),
        Err(_) => tracing::debug!(
            "Recording the sync status of {pipe} took longer than {} seconds.",
            RECORD_TIMEOUT.as_secs()
        ),
    }
}

/// Clears a pipe's `flush_scheduled` mark when its timer ends without doing so itself (its
/// runtime shut down), so the next held success schedules another.
struct TimerGuard {
    key: TrackKey,
    epoch: u64,
    armed: bool,
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        let retired = {
            let mut state = state();
            match state.tracks.get_mut(&self.key) {
                Some(track) if track.epoch == self.epoch => {
                    if self.armed {
                        track.flush_scheduled = false;
                    }
                    track.take_idle_target()
                }
                _ => None,
            }
        };
        drop(retired);
    }
}

/// Queue the held success of the pipe `key` when its window ends (`due`). Without a runtime the
/// held success goes out with the next record or read.
fn start_timer(key: TrackKey, due: Instant, epoch: u64) {
    let guard = TimerGuard {
        key: key.clone(),
        epoch,
        armed: true,
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(write_held(key, due, guard));
    }
}

/// The end of a debounce window: queue the held success, unless the pipe was forgotten since
/// (`guard.epoch`) or no longer exists on its instance (another process dropped or deleted it
/// within the window).
async fn write_held(key: TrackKey, due: Instant, mut guard: TimerGuard) {
    tokio::time::sleep_until(due.into()).await;
    let held = {
        let mut state = state();
        let Some(track) = state.tracks.get_mut(&key) else {
            return;
        };
        guard.armed = false;
        if track.epoch != guard.epoch {
            return;
        }
        track.flush_scheduled = false;
        let held = track.pending.take().zip(track.target.clone());
        if held.is_some() {
            track.last_write = Some(Instant::now());
            track.last_success = true;
        }
        held
    };
    let Some((outcome, (conn, pipe))) = held else {
        return;
    };
    if let Ok(Ok(false)) = tokio::time::timeout(RECORD_TIMEOUT, conn.pipe_exists(&pipe)).await {
        tracing::debug!("Not recording the held sync of {pipe}: the pipe no longer exists.");
        return;
    }
    let start = {
        let mut state = state();
        match state.tracks.get_mut(&key) {
            Some(track) if track.epoch == guard.epoch => {
                track.queue.push(outcome);
                claim_writer(track)
            }
            _ => false,
        }
    };
    if start {
        drop(start_writer(key));
    }
}

/// Write what this process holds for the pipes `which` selects (with `held`, their held
/// successes too) and wait until it is written, up to `timeout`.
async fn settle_where(which: impl Fn(&str) -> bool, held: bool, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let notified = changed().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let (busy, start) = {
            let mut state = state();
            let now = Instant::now();
            let mut busy = false;
            let mut start = Vec::new();
            for (key, track) in state.tracks.iter_mut() {
                if !which(&key.pipe_ref) {
                    continue;
                }
                if held && let Some(outcome) = track.pending.take() {
                    track.queue.push(outcome);
                    track.last_write = Some(now);
                    track.last_success = true;
                }
                if track.queue.is_empty() && !track.writing {
                    continue;
                }
                busy = true;
                if claim_writer(track) {
                    start.push(key.clone());
                }
            }
            (busy, start)
        };
        if !busy {
            return;
        }
        for key in start {
            if let Some(write) = start_writer(key) {
                write.await;
            }
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            tracing::debug!("Gave up waiting for sync status writes after {timeout:?}.");
            return;
        }
    }
}

/// Before reading the status of `pipe_ref`: write what this process holds for it, its held
/// success included, so a read right after a sync sees that sync (up to [`SETTLE_TIMEOUT`]).
pub async fn settle_pipe(pipe_ref: &str) {
    settle_where(|key| key == pipe_ref, true, SETTLE_TIMEOUT).await;
}

/// Before reading every status on `instance_keys`: [`settle_pipe`] for each of its pipes.
pub async fn settle_instance(instance_keys: &str) {
    let prefix = format!("{instance_keys}|");
    settle_where(|key| key.starts_with(&prefix), true, SETTLE_TIMEOUT).await;
}

/// Wait (up to `timeout`) until the outcomes this process queued for `pipe_refs` are written;
/// held successes stay held (the end of an engine run: a `--loop` keeps its debounce).
pub async fn wait_written(pipe_refs: &HashSet<String>, timeout: Duration) {
    settle_where(|key| pipe_refs.contains(key), false, timeout).await;
}

/// Whether this process holds outcomes it has not written yet (held, queued or being written).
pub fn has_unwritten() -> bool {
    crate::sync_metrics::has_unwritten()
        || state()
            .tracks
            .values()
            .any(|t| t.pending.is_some() || !t.queue.is_empty() || t.writing)
}

/// Write everything this process holds, held successes included, and wait until it is written
/// (up to `timeout`): what a process runs before it exits, so its latest syncs are kept.
pub async fn flush(timeout: Duration) {
    // Both independent writers get the same shutdown budget. Waiting for them in
    // sequence could consume twice the caller's timeout and starve a restarted metrics task.
    tokio::join!(
        settle_where(|_| true, true, timeout),
        crate::sync_metrics::flush(timeout),
    );
}

/// Forget what this process holds for the status of `pipe_ref` before its stored status is
/// cleared (the pipe was dropped, deleted or fully cleared): its held success and queued
/// outcomes are dropped, nothing decided before is written, and a write in progress is waited
/// for (up to [`RECORD_TIMEOUT`]). Keep the returned guard until the stored status is cleared, so
/// that no write of this process lands after it.
pub async fn forget(pipe_ref: &str) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    forget_tracks(pipe_ref, None).await
}

/// Forget only the status queue owned by `conn`; another physical instance may use the same alias.
pub async fn forget_on<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe_ref: &str,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    forget_tracks(pipe_ref, Some(TrackKey::new(conn, pipe_ref).owner)).await
}

async fn forget_tracks(
    pipe_ref: &str,
    owner: Option<usize>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    let retired = {
        let mut state = state();
        let epoch = state.new_epoch();
        let mut retired = Vec::new();
        for (key, track) in &mut state.tracks {
            if key.pipe_ref != pipe_ref || owner.is_some_and(|owner| key.owner != owner) {
                continue;
            }
            track.epoch = epoch;
            track.pending = None;
            track.queue.clear();
            track.flush_scheduled = false;
            track.last_write = None;
            track.last_success = false;
            if let Some(target) = track.take_idle_target() {
                retired.push(target);
            }
        }
        retired
    };
    drop(retired);
    changed().notify_waiters();
    let lock = write_lock(pipe_ref);
    tokio::time::timeout(RECORD_TIMEOUT, lock.lock_owned())
        .await
        .ok()
}

/// Store outcomes now, without the debounce or the background (the API server records what a
/// client posts, which the client debounced): bounded by [`RECORD_TIMEOUT`], errors as `Err`
/// text.
pub async fn record_now(
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    outcomes: &[SyncOutcome],
) -> Result<Option<SyncStatus>, String> {
    let lock = write_lock(&pipe.pipe_ref());
    // The wait for another write of this pipe is inside the time bound too.
    let write = async {
        let _guard = lock.lock().await;
        conn.record_sync_status(pipe, outcomes).await
    };
    match tokio::time::timeout(RECORD_TIMEOUT, write).await {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "recording the sync status took longer than {} seconds",
            RECORD_TIMEOUT.as_secs()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    fn outcome(success: bool, secs: i64) -> SyncOutcome {
        SyncOutcome {
            finished: at(secs),
            ..SyncOutcome::new(success, format!("o{secs}"), SyncSource::Cli, None)
        }
    }

    fn pipe() -> Pipe {
        Pipe::builder("sql:src", "weather")
            .location(Some("home"))
            .instance("sql:main")
            .build()
            .unwrap()
    }

    #[test]
    fn failures_count_until_a_success() {
        let s = SyncStatus::for_pipe(&pipe())
            .apply(&outcome(true, 0).with_counts(3, 1, 0))
            .apply(&outcome(false, 10))
            .apply(&outcome(false, 20));
        assert!(!s.success);
        assert_eq!(s.consecutive_failures, 2);
        assert_eq!(s.last_success, Some(at(0)));
        assert_eq!(s.last_failure, Some(at(20)));
        assert_eq!(s.last_attempt, Some(at(20)));
        assert_eq!((s.rows_inserted, s.rows_updated), (0, 0));
        let s = s.apply(&outcome(true, 30).with_counts(5, 0, 0));
        assert!(s.success);
        assert_eq!(s.consecutive_failures, 0);
        assert_eq!(s.last_success, Some(at(30)));
        assert_eq!(s.last_failure, Some(at(20)));
        assert_eq!(s.rows_inserted, 5);
        assert_eq!(s.message, "o30");
        assert_eq!(s.source, "cli");
        assert_eq!(s.pipe_ref, "sql:main|sql:src|weather|home");
    }

    #[test]
    fn an_older_outcome_only_moves_its_timestamp() {
        let s = SyncStatus::for_pipe(&pipe())
            .apply(&outcome(false, 20))
            .apply(&outcome(true, 10));
        assert!(!s.success, "the latest attempt (a failure) stays");
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(s.last_success, Some(at(10)));
        assert_eq!(s.last_attempt, Some(at(20)));
        assert_eq!(s.message, "o20");
    }

    #[test]
    fn rows_round_trip_and_json_has_the_contract_fields() {
        let s = SyncStatus::for_pipe(&pipe()).apply(
            &outcome(true, 0)
                .with_counts(1, 2, 3)
                .with_duration(Some(Duration::from_millis(1500))),
        );
        let back = status_from_doc(&status_doc(&s)).unwrap();
        assert_eq!(back, s);
        let j = s.to_json();
        for key in [
            "pipe_ref",
            "connector_keys",
            "metric_key",
            "location_key",
            "instance_keys",
            "last_attempt",
            "last_success",
            "last_failure",
            "success",
            "message",
            "rows_inserted",
            "rows_updated",
            "duration_seconds",
            "consecutive_failures",
            "source",
            "job",
        ] {
            assert!(j.get(key).is_some(), "{key}");
        }
        assert_eq!(j["last_attempt"], "2027-01-15T08:00:00.000000+00:00");
        assert!(j["last_failure"].is_null());
        assert_eq!(j["duration_seconds"], 1.5);
        let parsed: SyncStatus = serde_json::from_value(j).unwrap();
        assert_eq!(parsed, s);
    }

    #[test]
    fn rekey_rewrites_the_instance() {
        let s = rekey(SyncStatus::for_pipe(&pipe()), "api:main");
        assert_eq!(s.pipe_ref, "api:main|sql:src|weather|home");
        assert_eq!(s.instance_keys, "api:main");
    }

    #[test]
    fn long_messages_keep_both_ends() {
        let long = format!("{}{}", "a".repeat(2000), "Error: the end");
        let short = truncate_message(&long);
        assert!(short.chars().count() <= MAX_MESSAGE_CHARS + 3);
        assert!(short.starts_with("aaa"));
        assert!(short.ends_with("Error: the end"));
        assert!(short.contains('…'));
        assert_eq!(truncate_message("  ok  "), "ok");
        // Character boundaries, not bytes.
        let wide = "é".repeat(3000);
        assert!(truncate_message(&wide).chars().count() <= MAX_MESSAGE_CHARS + 3);
    }

    #[test]
    fn counts_come_from_sync_messages() {
        assert_eq!(
            message_counts("Inserted 1,024, updated 3 rows."),
            (1024, 3, 0)
        );
        assert_eq!(message_counts("Upserted 7 rows."), (0, 0, 7));
        assert_eq!(message_counts("Replaced 2 rows with 3 rows."), (3, 0, 0));
        assert_eq!(message_counts("Success"), (0, 0, 0));
    }

    #[test]
    fn successes_are_debounced_failures_never() {
        let window = Duration::from_secs(5);
        let t0 = Instant::now();
        let mut t = Track::default();
        // The first success writes.
        assert!(
            matches!(decide(&mut t, outcome(true, 0), t0, window), Decision::Write(b) if b.len() == 1)
        );
        // A second success inside the window is held, with one flush scheduled at the window end.
        assert_eq!(
            decide(
                &mut t,
                outcome(true, 1),
                t0 + Duration::from_secs(1),
                window
            ),
            Decision::Hold {
                schedule: Some(t0 + window)
            }
        );
        assert_eq!(
            decide(
                &mut t,
                outcome(true, 2),
                t0 + Duration::from_secs(2),
                window
            ),
            Decision::Hold { schedule: None }
        );
        // A failure writes at once, after the held success.
        match decide(
            &mut t,
            outcome(false, 3),
            t0 + Duration::from_secs(3),
            window,
        ) {
            Decision::Write(b) => {
                assert_eq!(b.len(), 2);
                assert!(b[0].success && b[0].message == "o2");
                assert!(!b[1].success);
            }
            other => panic!("{other:?}"),
        }
        // The first success after a failure writes at once.
        assert!(matches!(
            decide(&mut t, outcome(true, 4), t0 + Duration::from_secs(4), window),
            Decision::Write(b) if b.len() == 1
        ));
        // After the window a success writes again.
        assert!(matches!(
            decide(
                &mut t,
                outcome(true, 20),
                t0 + Duration::from_secs(20),
                window
            ),
            Decision::Write(_)
        ));
    }

    #[test]
    fn the_api_role_wins_then_jobs_then_graph_runs() {
        // (The role is process-global: only this test touches it.)
        let _scope = role_scope(SyncSource::Python);
        let job = current_job();
        let (source, name) = detect_source(true);
        match job {
            Some(j) => assert_eq!((source, name), (SyncSource::Job, Some(j))),
            None => {
                assert_eq!((source, name), (SyncSource::Engine, None));
                assert_eq!(detect_source(false), (SyncSource::Python, None));
            }
        }
        {
            let _api = role_scope(SyncSource::Api);
            assert_eq!(detect_source(true), (SyncSource::Api, None));
        }
        assert_eq!(process_role(), SyncSource::Python);
    }

    #[test]
    fn internal_and_temporary_pipes_have_no_status() {
        assert!(is_recorded(&pipe()));
        let internal = Pipe::builder("mrsm", "pipeline")
            .instance("sql:main")
            .build()
            .unwrap();
        assert!(!is_recorded(&internal));
        let mut temp = pipe();
        temp.temporary = true;
        assert!(!is_recorded(&temp));
    }

    // -----------------------------------------------------------------------------------------
    // Background writes, reads, forgetting and flushing, over an in-memory instance.

    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use crate::traits::{Connector, DateBound, PipeId, PipesKeys, PipesKeysQuery, async_trait};
    use meerschaum_core::SuccessTuple;
    use meerschaum_kernel::Frame;

    /// Tests that hold successes, or flush every pipe's, run one at a time.
    static HELD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// An instance that keeps statuses in memory and counts its finished writes; its writes may
    /// be slow, and it may say that its pipes are gone.
    #[derive(Default)]
    struct Fake {
        statuses: Mutex<HashMap<String, SyncStatus>>,
        writes: Arc<AtomicUsize>,
        write_delay: Duration,
        gone: AtomicBool,
        metrics: Mutex<HashMap<String, crate::sync_metrics::SyncMetric>>,
        metric_writes: AtomicUsize,
        hold_metric_ack: AtomicBool,
        metric_committed: tokio::sync::Notify,
        metric_ack: tokio::sync::Notify,
    }

    impl Fake {
        fn new() -> Arc<Fake> {
            Arc::new(Fake::default())
        }

        fn slow(delay: Duration) -> Arc<Fake> {
            Arc::new(Fake {
                write_delay: delay,
                ..Fake::default()
            })
        }

        fn writes(&self) -> usize {
            self.writes.load(Ordering::SeqCst)
        }

        fn stored(&self, pipe: &Pipe) -> Option<SyncStatus> {
            self.statuses.lock().unwrap().get(&pipe.pipe_ref()).cloned()
        }

        fn clear(&self) {
            self.statuses.lock().unwrap().clear();
        }
    }

    #[async_trait]
    impl Connector for Fake {
        fn connector_type(&self) -> &str {
            "fake"
        }

        fn label(&self) -> &str {
            "status"
        }

        async fn test_connection(&self) -> ConnectorResult<bool> {
            Ok(true)
        }
    }

    #[async_trait]
    impl InstanceConnector for Fake {
        async fn register_pipe(&self, _: &Pipe) -> SuccessTuple {
            SuccessTuple::success()
        }

        async fn get_pipe_attributes(&self, _: &Pipe) -> ConnectorResult<Map> {
            Ok(Map::new())
        }

        async fn get_pipe_id(&self, _: &Pipe) -> ConnectorResult<Option<PipeId>> {
            Ok(None)
        }

        async fn fetch_pipes_keys(&self, _: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
            Ok(PipesKeys::List(Vec::new()))
        }

        async fn pipe_exists(&self, _: &Pipe) -> ConnectorResult<bool> {
            Ok(!self.gone.load(Ordering::SeqCst))
        }

        async fn drop_pipe(&self, _: &Pipe) -> SuccessTuple {
            SuccessTuple::success()
        }

        async fn sync_pipe(&self, _: &Pipe, _: Frame, _: &SyncOptions) -> SuccessTuple {
            SuccessTuple::success()
        }

        async fn get_pipe_data(&self, _: &Pipe, _: &DataQuery) -> ConnectorResult<Option<Frame>> {
            Ok(None)
        }

        async fn get_sync_time(
            &self,
            _: &Pipe,
            _: Option<&Map>,
            _: bool,
        ) -> ConnectorResult<Option<DateBound>> {
            Ok(None)
        }

        async fn get_pipe_rowcount(&self, _: &Pipe, _: &DataQuery) -> ConnectorResult<Option<i64>> {
            Ok(None)
        }

        async fn get_pipe_columns_types(
            &self,
            _: &Pipe,
        ) -> ConnectorResult<IndexMap<String, String>> {
            Ok(IndexMap::new())
        }

        async fn record_sync_status(
            &self,
            pipe: &Pipe,
            outcomes: &[SyncOutcome],
        ) -> ConnectorResult<Option<SyncStatus>> {
            tokio::time::sleep(self.write_delay).await;
            let mut statuses = self.statuses.lock().unwrap();
            let base = statuses
                .remove(&pipe.pipe_ref())
                .unwrap_or_else(|| SyncStatus::for_pipe(pipe));
            let status = outcomes.iter().fold(base, SyncStatus::apply);
            statuses.insert(pipe.pipe_ref(), status.clone());
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(Some(status))
        }

        async fn record_sync_metrics(
            &self,
            metrics: &[crate::sync_metrics::SyncMetric],
        ) -> ConnectorResult<()> {
            self.metric_writes.fetch_add(1, Ordering::SeqCst);
            for metric in metrics {
                self.metrics
                    .lock()
                    .unwrap()
                    .insert(metric.event_id.clone(), metric.clone());
            }
            self.metric_committed.notify_one();
            if self.hold_metric_ack.load(Ordering::SeqCst) {
                self.metric_ack.notified().await;
            }
            Ok(())
        }
    }

    fn unique_pipe(tag: &str) -> Pipe {
        let metric = format!("{tag}_{}", uuid::Uuid::new_v4().simple());
        Pipe::builder("plugin:fake", &metric)
            .instance("fake:status")
            .build()
            .unwrap()
    }

    /// Wait for the writes queued for `key`, leaving its held success held.
    async fn written(key: &str) {
        settle_where(|k| k == key, false, SETTLE_TIMEOUT).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_record_returns_at_once_and_a_read_waits_for_its_write() {
        let fake = Fake::slow(Duration::from_millis(300));
        let pipe = unique_pipe("background");
        let started = Instant::now();
        record(fake.clone(), &pipe, outcome(false, 0));
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "the sync waited for its status"
        );
        assert_eq!(fake.writes(), 0);
        settle_pipe(&pipe.pipe_ref()).await;
        assert_eq!(fake.writes(), 1);
        let s = fake.stored(&pipe).unwrap();
        assert_eq!((s.success, s.consecutive_failures), (false, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_status_writes_release_transient_connectors() {
        let _serial = HELD.lock().await;
        let fake = Fake::slow(Duration::from_millis(20));
        let writes = fake.writes.clone();
        let owner = Arc::downgrade(&fake);
        let pipe = unique_pipe("transient_status_owner");
        record(fake.clone(), &pipe, outcome(false, 0));
        drop(fake);
        assert!(
            owner.upgrade().is_some(),
            "the queued write lost its connector"
        );
        written(&pipe.pipe_ref()).await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        for _ in 0..100 {
            if owner.strong_count() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            owner.upgrade().is_none(),
            "completed telemetry retained its connector"
        );
    }

    #[tokio::test]
    async fn held_status_keeps_its_connector_only_until_flushed() {
        let _serial = HELD.lock().await;
        let fake = Fake::new();
        let writes = fake.writes.clone();
        let owner = Arc::downgrade(&fake);
        let pipe = unique_pipe("held_transient_owner");
        let window = Duration::from_secs(60);
        record_in(fake.clone(), &pipe, outcome(true, 1), window);
        written(&pipe.pipe_ref()).await;
        record_in(fake.clone(), &pipe, outcome(true, 2), window);
        drop(fake);
        assert!(
            owner.upgrade().is_some(),
            "the held success lost its connector"
        );
        settle_pipe(&pipe.pipe_ref()).await;
        assert_eq!(writes.load(Ordering::SeqCst), 2);
        assert!(
            owner.upgrade().is_none(),
            "the flushed success retained its connector"
        );
    }

    #[tokio::test]
    async fn old_timer_cannot_flush_a_recreated_recording_track() {
        let _serial = HELD.lock().await;
        let fake = Fake::new();
        let pipe = unique_pipe("recreated_track");
        let key = TrackKey::new(fake.as_ref(), &pipe.pipe_ref());
        let window = Duration::from_secs(60);
        record_in(fake.clone(), &pipe, outcome(true, 1), window);
        written(&pipe.pipe_ref()).await;
        record_in(fake.clone(), &pipe, outcome(true, 2), window);
        let old_epoch = state().tracks[&key].epoch;
        settle_pipe(&pipe.pipe_ref()).await;
        // Recreate the same key, as when an allocator reuses a retired connector's address.
        state().tracks.remove(&key);
        record_in(fake.clone(), &pipe, outcome(true, 3), window);
        written(&pipe.pipe_ref()).await;
        record_in(fake.clone(), &pipe, outcome(true, 4), window);
        write_held(
            key.clone(),
            Instant::now(),
            TimerGuard {
                key,
                epoch: old_epoch,
                armed: true,
            },
        )
        .await;
        written(&pipe.pipe_ref()).await;
        assert_eq!(
            fake.writes(),
            3,
            "an old timer flushed the new owner's held success"
        );
        assert_eq!(fake.stored(&pipe).unwrap().message, "o3");
        settle_pipe(&pipe.pipe_ref()).await;
        assert_eq!(fake.stored(&pipe).unwrap().message, "o4");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outcomes_queued_during_a_write_go_out_together_in_order() {
        let fake = Fake::slow(Duration::from_millis(200));
        let pipe = unique_pipe("queued");
        record(fake.clone(), &pipe, outcome(false, 0));
        // The first write runs; two more failures queue up behind it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        record(fake.clone(), &pipe, outcome(false, 1));
        record(fake.clone(), &pipe, outcome(false, 2));
        written(&pipe.pipe_ref()).await;
        assert_eq!(fake.writes(), 2, "the queued failures are one write");
        let s = fake.stored(&pipe).unwrap();
        assert_eq!(s.consecutive_failures, 3);
        assert_eq!(s.message, "o2");
    }

    #[test]
    fn identical_aliases_keep_their_queued_outcomes_on_their_own_connector() {
        let first = Fake::new();
        let second = Fake::new();
        let pipe = unique_pipe("same_alias_queued");
        // No runtime may consume either queue before both targets have been recorded.
        record(first.clone(), &pipe, outcome(false, 1));
        record(second.clone(), &pipe, outcome(false, 2));
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            settle_pipe(&pipe.pipe_ref()).await;
        });
        let first_status = first.stored(&pipe).unwrap();
        let second_status = second.stored(&pipe).unwrap();
        assert_eq!(first_status.message, "o1");
        assert_eq!(second_status.message, "o2");
        assert_eq!(first_status.consecutive_failures, 1);
        assert_eq!(second_status.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn identical_aliases_do_not_share_debounce_or_deletion_state() {
        let _serial = HELD.lock().await;
        let first = Fake::new();
        let second = Fake::new();
        let pipe = unique_pipe("same_alias_held");
        let window = Duration::from_secs(60);
        record_in(first.clone(), &pipe, outcome(true, 1), window);
        written(&pipe.pipe_ref()).await;
        record_in(first.clone(), &pipe, outcome(true, 2), window);
        record_in(second.clone(), &pipe, outcome(true, 3), window);
        written(&pipe.pipe_ref()).await;
        assert_eq!(first.stored(&pipe).unwrap().message, "o1");
        assert_eq!(second.stored(&pipe).unwrap().message, "o3");
        record_in(second.clone(), &pipe, outcome(true, 4), window);
        let guard = forget_on(first.as_ref(), &pipe.pipe_ref()).await;
        first.clear();
        drop(guard);
        settle_pipe(&pipe.pipe_ref()).await;
        assert!(
            first.stored(&pipe).is_none(),
            "the deleted instance stays clear"
        );
        assert_eq!(second.stored(&pipe).unwrap().message, "o4");
    }

    #[test]
    fn cancelled_metrics_acknowledgement_is_retried_without_changing_event_identity() {
        let _serial = HELD.blocking_lock();
        let fake = Fake::new();
        fake.hold_metric_ack.store(true, Ordering::SeqCst);
        let pipe = unique_pipe("cancelled_metrics");
        pipe.set_registration_id(Some("synthetic-registration".into()));
        let first = tokio::runtime::Runtime::new().unwrap();
        first.block_on(async {
            crate::sync_metrics::record(fake.clone(), &pipe, &outcome(true, 1));
            tokio::time::timeout(Duration::from_secs(1), fake.metric_committed.notified())
                .await
                .unwrap();
        });
        let event_id = fake.metrics.lock().unwrap().keys().next().unwrap().clone();
        drop(first);
        assert!(crate::sync_metrics::has_unwritten());
        fake.hold_metric_ack.store(false, Ordering::SeqCst);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            crate::sync_metrics::flush(Duration::from_secs(1)).await;
        });
        assert!(!crate::sync_metrics::has_unwritten());
        assert_eq!(fake.metric_writes.load(Ordering::SeqCst), 2);
        let rows = fake.metrics.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows.contains_key(&event_id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forgetting_a_pipe_waits_for_its_write_and_drops_what_is_queued() {
        let fake = Fake::slow(Duration::from_millis(300));
        let pipe = unique_pipe("forget");
        let key = pipe.pipe_ref();
        record(fake.clone(), &pipe, outcome(false, 0));
        tokio::time::sleep(Duration::from_millis(50)).await;
        record(fake.clone(), &pipe, outcome(false, 1));
        let guard = forget(&key).await;
        assert!(guard.is_some());
        assert_eq!(fake.writes(), 1, "the write in progress finished first");
        // The caller clears the stored status while it holds the guard.
        fake.clear();
        drop(guard);
        settle_pipe(&key).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(fake.writes(), 1, "the queued failure was dropped");
        assert!(fake.stored(&pipe).is_none());

        // The pipe records again afterwards, from scratch.
        record(fake.clone(), &pipe, outcome(false, 2));
        settle_pipe(&key).await;
        assert_eq!(fake.stored(&pipe).unwrap().consecutive_failures, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forgetting_a_pipe_drops_its_held_success() {
        let _serial = HELD.lock().await;
        let fake = Fake::new();
        let pipe = unique_pipe("forget_held");
        let key = pipe.pipe_ref();
        let window = Duration::from_millis(300);
        record_in(fake.clone(), &pipe, outcome(true, 0), window);
        written(&key).await;
        record_in(fake.clone(), &pipe, outcome(true, 1), window);
        assert_eq!(fake.writes(), 1, "the second success is held");
        drop(forget(&key).await);
        fake.clear();
        // The window ends and a read follows: nothing brings the status back.
        tokio::time::sleep(window * 3).await;
        settle_pipe(&key).await;
        assert_eq!(fake.writes(), 1);
        assert!(fake.stored(&pipe).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_success_is_written_when_its_window_ends_unless_its_pipe_is_gone() {
        let _serial = HELD.lock().await;
        let fake = Fake::new();
        let pipe = unique_pipe("held");
        let key = pipe.pipe_ref();
        let window = Duration::from_millis(300);
        record_in(fake.clone(), &pipe, outcome(true, 0), window);
        record_in(fake.clone(), &pipe, outcome(true, 1), window);
        written(&key).await;
        assert_eq!(fake.writes(), 1);
        assert_eq!(fake.stored(&pipe).unwrap().message, "o0");
        tokio::time::sleep(window * 3).await;
        assert_eq!(fake.writes(), 2, "written when the window ended");
        assert_eq!(fake.stored(&pipe).unwrap().message, "o1");

        // Another process deletes the pipe within a window: its held success is dropped.
        record_in(fake.clone(), &pipe, outcome(true, 2), window);
        written(&key).await;
        record_in(fake.clone(), &pipe, outcome(true, 3), window);
        let before = fake.writes();
        fake.gone.store(true, Ordering::SeqCst);
        tokio::time::sleep(window * 3).await;
        assert_eq!(fake.writes(), before);
        assert_eq!(fake.stored(&pipe).unwrap().message, "o2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_and_a_flush_write_held_successes() {
        let _serial = HELD.lock().await;
        let fake = Fake::new();
        let window = Duration::from_secs(60);

        // A read of the pipe writes its held success first.
        let read = unique_pipe("read");
        record_in(fake.clone(), &read, outcome(true, 0), window);
        record_in(fake.clone(), &read, outcome(true, 1), window);
        settle_pipe(&read.pipe_ref()).await;
        assert_eq!(fake.stored(&read).unwrap().message, "o1");

        // So does a read of its instance, and the flush before a process exits.
        let bulk = unique_pipe("bulk");
        record_in(fake.clone(), &bulk, outcome(true, 0), window);
        record_in(fake.clone(), &bulk, outcome(true, 1), window);
        settle_instance("fake:status").await;
        assert_eq!(fake.stored(&bulk).unwrap().message, "o1");
        let exiting = unique_pipe("exiting");
        record_in(fake.clone(), &exiting, outcome(true, 0), window);
        record_in(fake.clone(), &exiting, outcome(true, 1), window);
        flush(Duration::from_secs(2)).await;
        assert_eq!(fake.stored(&exiting).unwrap().message, "o1");
        let state = state();
        let track = &state.tracks[&TrackKey::new(fake.as_ref(), &exiting.pipe_ref())];
        assert!(track.pending.is_none() && track.queue.is_empty() && !track.writing);
    }

    #[test]
    fn a_writer_lost_with_its_runtime_is_replaced() {
        let fake = Fake::slow(Duration::from_millis(1_000));
        let pipe = unique_pipe("lost");
        let key = pipe.pipe_ref();
        let runtime = || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
        };
        let first = runtime();
        first.block_on(async {
            record(fake.clone(), &pipe, outcome(false, 0));
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        // The runtime goes away in the middle of the write: the write is lost with it.
        drop(first);
        assert!(
            !state().tracks[&TrackKey::new(fake.as_ref(), &key)].writing,
            "the lost writer let go"
        );
        runtime().block_on(async {
            record(fake.clone(), &pipe, outcome(false, 1));
            settle_pipe(&key).await;
        });
        assert_eq!(fake.writes(), 1);
        assert_eq!(fake.stored(&pipe).unwrap().message, "o1");
    }
}
