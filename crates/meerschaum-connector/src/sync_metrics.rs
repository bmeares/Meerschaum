//! Bounded, numeric sync history shared by ordinary syncs, engine runs, SQL and Valkey.
//! Recording never waits on a database; persisted rows survive process and API worker restarts.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::pipe_ops::Doc;
use crate::sync_status::{
    SyncOutcome, doc_bool, doc_dt, doc_f64, doc_str, doc_u64, static_context,
};
use crate::{
    ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, SyncOptions,
};

/// The internal history pipe, placed by the instance beside its other engine state.
pub const METRIC: &str = "engine_metrics";
/// Persisted history expires after this many days.
pub const RETENTION_DAYS: i64 = 30;
/// Maximum outcomes waiting in this process; overload drops telemetry, never the sync.
pub const MAX_PENDING: usize = 4096;
/// Largest database write batch.
pub const BATCH_SIZE: usize = 128;
/// Maximum sync rows returned or aggregated by one request.
pub const MAX_ROWS: usize = 5000;
/// Maximum run rows considered by one request.
pub const MAX_RUNS: usize = 1000;
/// Maximum node rows considered by one request.
pub const MAX_NODES: usize = 10000;
/// Database time budget of one background write.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(3);

/// Stable identity within an instance; aliases of the instance share the same key.
pub fn pipe_key(connector: &str, metric: &str, location: Option<&str>) -> String {
    let identity = serde_json::to_vec(&(connector, metric, location)).unwrap_or_default();
    hex::encode(Sha256::digest(identity))
}

/// Persisted ID plus normalized ACL. Access changes start a new visible metrics segment.
pub fn registration_stamp(identity: &str, parameters: &Map) -> String {
    let access = meerschaum_core::access::PipeAccess::from_parameters(parameters).to_value();
    hex::encode(Sha256::digest(
        format!("{identity}|{}", access.json_dumps()).as_bytes(),
    ))
}

/// Registration segment observed during an existing metadata read, without another query.
pub fn observed_registration(pipe: &Pipe) -> Option<String> {
    pipe.registration_id()
        .map(|identity| registration_stamp(&identity, pipe.raw_parameters()))
}

/// Current registration segments, in one fresh bulk metadata query.
pub async fn registrations<C: InstanceConnector + ?Sized>(
    conn: &C,
) -> ConnectorResult<HashMap<String, String>> {
    Ok(registration_snapshot(conn).await?.0)
}

async fn registration_snapshot<C: InstanceConnector + ?Sized>(
    conn: &C,
) -> ConnectorResult<(HashMap<String, String>, HashMap<String, String>)> {
    let keys = conn
        .registered_pipes(&crate::PipesKeysQuery::default())
        .await?;
    let mut current = HashMap::new();
    let mut pipelines = HashMap::new();
    for (identity, entry) in keys {
        if let Some(identity) = identity {
            let parameters = match entry.extra {
                Some(Value::Map(parameters)) => parameters,
                Some(Value::Str(text)) => Value::from_json_str(&text)
                    .ok()
                    .map(|value| value.to_map())
                    .unwrap_or_default(),
                _ => Map::new(),
            };
            if entry.connector_keys == "mrsm" {
                if entry.metric_key == "pipeline"
                    && let (Some(name), Some(identity)) = (
                        entry.location_key,
                        parameters
                            .get("pipeline")
                            .and_then(|definition| definition.get("id"))
                            .and_then(Value::as_str),
                    )
                {
                    pipelines.insert(name, identity.to_string());
                }
                continue;
            }
            current.insert(
                pipe_key(
                    &entry.connector_keys,
                    &entry.metric_key,
                    entry.location_key.as_deref(),
                ),
                registration_stamp(&identity.to_string(), &parameters),
            );
        }
    }
    let saved = conn
        .registered_pipes(&crate::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await?;
    for (_, entry) in saved {
        let parameters = match &entry.extra {
            Some(Value::Map(parameters)) => parameters.clone(),
            Some(Value::Str(text)) => Value::from_json_str(text)
                .ok()
                .map(|value| value.to_map())
                .unwrap_or_default(),
            _ => Map::new(),
        };
        if let (Some(name), Some(identity)) = (
            &entry.location_key,
            parameters
                .get("pipeline")
                .and_then(|definition| definition.get("id"))
                .and_then(Value::as_str),
        ) {
            pipelines.insert(name.clone(), identity.to_string());
        }
    }
    Ok((current, pipelines))
}

/// A pipe reference with or without its instance prefix.
pub fn reference_keys(reference: &str) -> Option<(&str, &str, Option<&str>)> {
    let fields: Vec<&str> = reference.split('|').collect();
    let (connector, metric, location) = match fields.as_slice() {
        [_, connector, metric, location] | [connector, metric, location] => {
            (*connector, *metric, Some(*location))
        }
        [connector, metric] => (*connector, *metric, None),
        _ => return None,
    };
    if connector.is_empty() || metric.is_empty() || connector == "mrsm" {
        return None;
    }
    Some((
        connector,
        metric,
        location.filter(|value| !matches!(*value, "None" | "")),
    ))
}

/// Numeric metadata of one completed sync. Errors, SQL, parameters and job labels are excluded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncMetric {
    /// Collision-resistant event identity, retained across transport retries.
    pub event_id: String,
    /// Completion time, UTC.
    pub ts: DateTime<Utc>,
    /// Start time, UTC.
    pub started: DateTime<Utc>,
    /// Instance-independent key used by indexed visibility filters.
    pub pipe_key: String,
    /// Persisted registration and ACL segment at capture time.
    pub registration_id: String,
    /// Display identity, re-keyed on reads under the requested instance.
    pub pipe_ref: String,
    /// Instance label.
    pub instance_keys: String,
    /// Source connector keys.
    pub connector_keys: String,
    /// Metric key.
    pub metric_key: String,
    /// Location key.
    pub location_key: Option<String>,
    /// Associated engine run, when known.
    pub run_id: Option<String>,
    /// Final outcome.
    pub success: bool,
    /// Elapsed seconds.
    pub duration_seconds: f64,
    /// Rows inserted.
    pub inserted: u64,
    /// Rows updated.
    pub updated: u64,
    /// Rows upserted.
    pub upserted: u64,
    /// Rows deleted.
    pub deleted: u64,
    /// Rows changed (inserted + updated + upserted + deleted).
    pub rows: u64,
    /// Attempts, including the first.
    pub attempts: u64,
    /// Chunks completed.
    pub chunks: u64,
    /// Native caller kind; no arbitrary process labels.
    pub source: String,
}

impl SyncMetric {
    /// Convert an existing native outcome without retaining its error text.
    pub fn from_outcome(pipe: &Pipe, outcome: &SyncOutcome) -> Self {
        let duration = outcome
            .duration_seconds
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or(0.0)
            .min(RETENTION_DAYS as f64 * 86400.0);
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            ts: outcome.finished,
            started: outcome.finished - TimeDelta::milliseconds((duration * 1000.0) as i64),
            pipe_key: pipe_key(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
            ),
            registration_id: observed_registration(pipe).unwrap_or_default(),
            pipe_ref: pipe.pipe_ref(),
            instance_keys: pipe.instance_keys().into(),
            connector_keys: pipe.connector_keys().into(),
            metric_key: pipe.metric_key().into(),
            location_key: pipe.location_key().map(str::to_string),
            run_id: outcome.run_id.clone(),
            success: outcome.success,
            duration_seconds: duration,
            inserted: outcome.inserted,
            updated: outcome.updated,
            upserted: outcome.upserted,
            deleted: outcome.deleted,
            rows: outcome
                .inserted
                .saturating_add(outcome.updated)
                .saturating_add(outcome.upserted)
                .saturating_add(outcome.deleted),
            attempts: u64::from(outcome.attempts.max(1)),
            chunks: outcome.chunks,
            source: outcome.source.as_str().into(),
        }
    }

    /// Canonicalize untrusted transport metadata on the server's selected instance.
    pub fn canonicalize(&mut self, pipe: &Pipe) -> ConnectorResult<()> {
        if uuid::Uuid::parse_str(&self.event_id).is_err()
            || !self.duration_seconds.is_finite()
            || self.duration_seconds < 0.0
            || self.duration_seconds > RETENTION_DAYS as f64 * 86400.0
            || self.ts > Utc::now() + TimeDelta::minutes(5)
            || self.ts < Utc::now() - TimeDelta::days(RETENTION_DAYS)
        {
            return Err(ConnectorError::Invalid(
                "Invalid sync metric bounds or identity.".into(),
            ));
        }
        self.pipe_key = pipe_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        self.pipe_ref = pipe.pipe_ref();
        self.instance_keys = pipe.instance_keys().into();
        self.connector_keys = pipe.connector_keys().into();
        self.metric_key = pipe.metric_key().into();
        self.location_key = pipe.location_key().map(str::to_string);
        self.started = self.ts - TimeDelta::milliseconds((self.duration_seconds * 1000.0) as i64);
        self.attempts = self.attempts.clamp(1, 1_000_000);
        self.chunks = self.chunks.min(i64::MAX as u64);
        self.inserted = self.inserted.min(i64::MAX as u64);
        self.updated = self.updated.min(i64::MAX as u64);
        self.upserted = self.upserted.min(i64::MAX as u64);
        self.deleted = self.deleted.min(i64::MAX as u64);
        self.rows = self
            .inserted
            .saturating_add(self.updated)
            .saturating_add(self.upserted)
            .saturating_add(self.deleted)
            .min(i64::MAX as u64);
        self.source = crate::sync_status::SyncSource::parse(&self.source)
            .map(|source| source.as_str().to_string())
            .unwrap_or_else(|| "api".into());
        Ok(())
    }

    /// Row in the internal metric pipe.
    pub fn to_doc(&self) -> Doc {
        let mut doc: Doc = serde_json::to_value(self)
            .unwrap_or_default()
            .as_object()
            .into_iter()
            .flat_map(|object| object.iter())
            .map(|(key, value)| {
                (
                    key.clone(),
                    meerschaum_kernel::frame::json_to_value(value.clone()),
                )
            })
            .collect();
        doc.insert(
            "ts_us".into(),
            meerschaum_kernel::value::Value::Int(self.ts.timestamp_micros()),
        );
        doc
    }

    /// Restore a typed row from any native instance backend.
    pub fn from_doc(doc: &Doc) -> Option<Self> {
        Some(Self {
            event_id: doc_str(doc, "event_id")?,
            ts: doc_dt(doc, "ts")?,
            started: doc_dt(doc, "started")?,
            pipe_key: doc_str(doc, "pipe_key")?,
            registration_id: doc_str(doc, "registration_id")?,
            pipe_ref: doc_str(doc, "pipe_ref")?,
            instance_keys: doc_str(doc, "instance_keys")?,
            connector_keys: doc_str(doc, "connector_keys")?,
            metric_key: doc_str(doc, "metric_key")?,
            location_key: doc_str(doc, "location_key"),
            run_id: doc_str(doc, "run_id"),
            success: doc_bool(doc, "success"),
            duration_seconds: doc_f64(doc, "duration_seconds").unwrap_or(0.0),
            inserted: doc_u64(doc, "inserted"),
            updated: doc_u64(doc, "updated"),
            upserted: doc_u64(doc, "upserted"),
            deleted: doc_u64(doc, "deleted"),
            rows: doc_u64(doc, "rows"),
            attempts: doc_u64(doc, "attempts").max(1),
            chunks: doc_u64(doc, "chunks"),
            source: doc_str(doc, "source").unwrap_or_default(),
        })
    }
}

/// Bounded reader request. Visibility ceilings are supplied only by trusted local callers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsQuery {
    /// Lower completion time, inclusive; defaults to the last day.
    pub begin: Option<DateTime<Utc>>,
    /// Upper completion time, exclusive; defaults to now.
    pub end: Option<DateTime<Utc>>,
    /// Exact Pipe reference.
    #[serde(alias = "pipe")]
    pub pipe_ref: Option<String>,
    /// Saved Pipeline name.
    pub pipeline: Option<String>,
    /// Maximum sync rows; at most `MAX_ROWS`.
    pub limit: Option<usize>,
    /// Time buckets; at most 200.
    pub buckets: Option<usize>,
    /// Visible Pipe refs. `Some([])` means no access; `None` is trusted local access.
    pub visible_refs: Option<Vec<String>>,
    /// Visible saved Pipelines, with the same empty/None semantics.
    pub visible_pipelines: Option<Vec<String>>,
}

impl MetricsQuery {
    /// Validate bounds without constructing a connector or reading any backend.
    pub fn validate(&self) -> ConnectorResult<()> {
        query_bounds(self).map(|_| ())
    }
}

/// Aggregate numeric outcomes in a bounded time range.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricSummary {
    /// Completed syncs (or runs for a Pipeline summary).
    pub syncs: u64,
    /// Successful outcomes.
    pub succeeded: u64,
    /// Failed outcomes.
    pub failed: u64,
    /// Success fraction, 0 through 1; null with no completed outcomes.
    pub success_rate: Option<f64>,
    /// Rows inserted.
    pub inserted: u64,
    /// Rows updated.
    pub updated: u64,
    /// Rows upserted.
    pub upserted: u64,
    /// Rows deleted.
    pub deleted: u64,
    /// Rows changed (inserted + updated + upserted + deleted).
    pub rows: u64,
    /// Attempts, including first attempts.
    pub attempts: u64,
    /// Retries after first attempts.
    pub retries: u64,
    /// Completed chunks.
    pub chunks: u64,
    /// Total measured duration, seconds.
    pub duration_seconds: f64,
    /// Mean duration, seconds; null without outcomes.
    pub avg_duration_seconds: Option<f64>,
    /// Rows per measured second; null without elapsed time.
    pub rows_per_second: Option<f64>,
    /// Latest completion time.
    pub last_sync: Option<DateTime<Utc>>,
    /// Latest success time.
    pub last_success: Option<DateTime<Utc>>,
    /// Latest failure time.
    pub last_failure: Option<DateTime<Utc>>,
}

impl MetricSummary {
    /// Add one already-authorized outcome.
    pub fn add(&mut self, metric: &SyncMetric) {
        self.syncs += 1;
        self.succeeded += u64::from(metric.success);
        self.failed += u64::from(!metric.success);
        self.inserted = self.inserted.saturating_add(metric.inserted);
        self.updated = self.updated.saturating_add(metric.updated);
        self.upserted = self.upserted.saturating_add(metric.upserted);
        self.deleted = self.deleted.saturating_add(metric.deleted);
        self.rows = self
            .inserted
            .saturating_add(self.updated)
            .saturating_add(self.upserted)
            .saturating_add(self.deleted);
        self.attempts = self.attempts.saturating_add(metric.attempts);
        self.retries = self
            .retries
            .saturating_add(metric.attempts.saturating_sub(1));
        self.chunks = self.chunks.saturating_add(metric.chunks);
        self.duration_seconds += metric.duration_seconds;
        self.success_rate = Some(self.succeeded as f64 / self.syncs as f64);
        self.avg_duration_seconds = Some(self.duration_seconds / self.syncs as f64);
        self.rows_per_second =
            (self.duration_seconds > 0.0).then(|| self.rows as f64 / self.duration_seconds);
        self.last_sync = self.last_sync.max(Some(metric.ts));
        if metric.success {
            self.last_success = self.last_success.max(Some(metric.ts));
        } else {
            self.last_failure = self.last_failure.max(Some(metric.ts));
        }
    }
}

/// One time bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricBucket {
    /// Inclusive bucket start, UTC.
    pub ts: DateTime<Utc>,
    /// Bucket aggregate.
    #[serde(flatten)]
    pub summary: MetricSummary,
}

/// One Pipe's aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipeMetricSummary {
    /// Pipe identity.
    pub pipe_ref: String,
    /// Source connector keys.
    pub connector_keys: String,
    /// Metric key.
    pub metric_key: String,
    /// Location key.
    pub location_key: Option<String>,
    /// Numeric results.
    pub summary: MetricSummary,
}

/// One saved Pipeline's run aggregate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineMetricSummary {
    /// Saved Pipeline name.
    pub name: String,
    /// Completed run aggregates; running runs do not count as completed syncs.
    pub summary: MetricSummary,
    /// Runs still in progress.
    pub running: u64,
    /// Most recent visible run.
    pub last_run_id: Option<String>,
    /// Its start time.
    pub last_started: Option<DateTime<Utc>>,
    /// Its completion time.
    pub last_finished: Option<DateTime<Utc>>,
    /// Its outcome, null while running.
    pub last_success: Option<bool>,
}

/// A bounded numeric view, shared by the SDK, HTTP API and console.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    /// Applied lower bound, UTC.
    pub begin: DateTime<Utc>,
    /// Applied upper bound, UTC.
    pub end: DateTime<Utc>,
    /// Applied sync row limit.
    pub limit: usize,
    /// The requested window contains more rows than this bounded view.
    pub truncated: bool,
    /// Storage retention duration.
    pub retention_days: i64,
    /// Numeric Pipe-sync aggregate.
    pub summary: MetricSummary,
    /// Fixed time buckets.
    pub series: Vec<MetricBucket>,
    /// Per-Pipe aggregates.
    pub pipes: Vec<PipeMetricSummary>,
    /// Per-Pipeline run aggregates.
    pub pipelines: Vec<PipelineMetricSummary>,
    /// Individual numeric outcomes, newest first.
    pub history: Vec<SyncMetric>,
}

/// Internal storage descriptor, including flavor-specific schema and namespace.
pub async fn metrics_pipe<C: InstanceConnector + ?Sized>(conn: &C) -> ConnectorResult<Pipe> {
    conn.engine_state_pipe(&conn.connector_keys(), METRIC)
        .await?
        .ok_or_else(|| {
            ConnectorError::Invalid("The instance does not support sync metrics.".into())
        })
}

/// Create an empty metrics table and its indexes. Readers never create state as a side effect.
pub async fn initialize<C: InstanceConnector + ?Sized>(conn: &C) -> ConnectorResult<()> {
    for metric in [METRIC, "engine_runs", "engine_nodes"] {
        let Some(pipe) = conn
            .engine_state_pipe(&conn.connector_keys(), metric)
            .await?
        else {
            continue;
        };
        if !conn.pipe_exists(&pipe).await? {
            let frame = pipe_enforce_dtypes(PipeInput::Docs(Vec::new()), &static_context(&pipe))
                .map_err(|error| ConnectorError::Invalid(error.to_string()))?;
            let result = conn.sync_pipe(&pipe, frame, &write_options()).await;
            if !result.success {
                return Err(ConnectorError::Backend(result.message));
            }
        }
        if conn.connector_type() == "sql" {
            let result = conn.create_pipe_indices(&pipe).await;
            if !result.success {
                return Err(ConnectorError::Backend(result.message));
            }
        }
    }
    Ok(())
}

fn write_options() -> SyncOptions {
    let mut extra = Map::new();
    extra.insert("enforce".into(), Value::Bool(false));
    extra.insert("_check_temporary_tables".into(), Value::Bool(false));
    SyncOptions {
        check_existing: true,
        extra,
        ..Default::default()
    }
}

/// Persist a bounded batch using the existing native internal-Pipe backend.
pub async fn store<C: InstanceConnector + ?Sized>(
    conn: &C,
    metrics: &[SyncMetric],
) -> ConnectorResult<()> {
    if metrics.is_empty() {
        return Ok(());
    }
    let pipe = metrics_pipe(conn).await?;
    let frame = pipe_enforce_dtypes(
        PipeInput::Docs(metrics.iter().map(SyncMetric::to_doc).collect()),
        &static_context(&pipe),
    )
    .map_err(|error| ConnectorError::Invalid(error.to_string()))?;
    let result = conn.sync_pipe(&pipe, frame, &write_options()).await;
    if result.success {
        prune(conn).await;
        Ok(())
    } else {
        Err(ConnectorError::Backend(result.message))
    }
}

/// Expire history through indexed timestamps, with bounded per-document cleanup where needed.
pub async fn prune<C: InstanceConnector + ?Sized>(conn: &C) {
    let due = if conn.connector_type() == "valkey" {
        // Its bounded per-document cleanup must keep pace with incoming write batches.
        true
    } else {
        let mut state = recorder();
        let key = format!("{}:{conn:p}", conn.connector_keys());
        if state
            .pruned
            .get(&key)
            .is_none_or(|last| last.elapsed() > Duration::from_secs(60))
        {
            if state.pruned.len() >= MAX_PENDING {
                state.pruned.clear();
            }
            state.pruned.insert(key, Instant::now());
            true
        } else {
            false
        }
    };
    if !due {
        return;
    }
    let _ = tokio::time::timeout(WRITE_TIMEOUT, async {
        conn.expire_engine_metrics(
            (Utc::now() - TimeDelta::days(RETENTION_DAYS)).timestamp_micros(),
        )
        .await
    })
    .await;
}

struct Pending {
    conn: Arc<dyn InstanceConnector>,
    metric: SyncMetric,
}

#[derive(Default)]
struct Recorder {
    pid: u32,
    pending: VecDeque<Pending>,
    running: bool,
    pruned: HashMap<String, Instant>,
}

fn recorder() -> std::sync::MutexGuard<'static, Recorder> {
    static STATE: OnceLock<Mutex<Recorder>> = OnceLock::new();
    let mut state = STATE
        .get_or_init(|| Mutex::new(Recorder::default()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if state.pid != std::process::id() {
        *state = Recorder {
            pid: std::process::id(),
            ..Default::default()
        };
    }
    state
}

fn changed() -> &'static tokio::sync::Notify {
    static NOTIFY: OnceLock<tokio::sync::Notify> = OnceLock::new();
    NOTIFY.get_or_init(tokio::sync::Notify::new)
}

/// Queue metadata without blocking the original sync or retaining unbounded history in memory.
pub fn record(conn: Arc<dyn InstanceConnector>, pipe: &Pipe, outcome: &SyncOutcome) {
    if !crate::sync_status::is_recorded(pipe) || observed_registration(pipe).is_none() {
        return;
    }
    {
        let mut state = recorder();
        if state.pending.len() == MAX_PENDING {
            state.pending.pop_front();
            tracing::debug!("Sync metric queue is full; dropped oldest telemetry outcome.");
        }
        state.pending.push_back(Pending {
            conn,
            metric: SyncMetric::from_outcome(pipe, outcome),
        });
    }
    start_writer();
}

struct WriterGuard {
    armed: bool,
}
impl Drop for WriterGuard {
    fn drop(&mut self) {
        if self.armed {
            recorder().running = false;
            changed().notify_waiters();
        }
    }
}

fn start_writer() {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let mut state = recorder();
    if state.running || state.pending.is_empty() {
        return;
    }
    state.running = true;
    let guard = WriterGuard { armed: true };
    drop(state);
    handle.spawn(async move {
        let mut guard = guard;
        loop {
            let batch: Vec<_> = {
                let mut state = recorder();
                let count = BATCH_SIZE.min(state.pending.len());
                if count == 0 {
                    state.running = false;
                    guard.armed = false;
                    changed().notify_waiters();
                    return;
                }
                state.pending.drain(..count).collect()
            };
            let mut groups: HashMap<usize, (Arc<dyn InstanceConnector>, Vec<SyncMetric>)> =
                HashMap::new();
            for pending in batch {
                let identity = Arc::as_ptr(&pending.conn) as *const () as usize;
                groups
                    .entry(identity)
                    .or_insert_with(|| (pending.conn.clone(), Vec::new()))
                    .1
                    .push(pending.metric);
            }
            for (_, (conn, metrics)) in groups {
                match tokio::time::timeout(WRITE_TIMEOUT, conn.record_sync_metrics(&metrics)).await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::debug!("Cannot record sync metrics: {error}"),
                    Err(_) => tracing::debug!("Sync metrics write exceeded its time budget."),
                }
            }
            changed().notify_waiters();
            tokio::task::yield_now().await;
        }
    });
}

/// Whether this process still owns unwritten metadata.
pub fn has_unwritten() -> bool {
    let state = recorder();
    state.running || !state.pending.is_empty()
}

/// Flush with a bounded wait for explicit checks and graceful process shutdown.
pub async fn flush(timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let notified = changed().notified();
        start_writer();
        if !has_unwritten() {
            return;
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            return;
        }
    }
}

fn query_bounds(query: &MetricsQuery) -> ConnectorResult<(DateTime<Utc>, DateTime<Utc>, usize)> {
    let end = query.end.unwrap_or_else(Utc::now);
    let begin = query.begin.unwrap_or(end - TimeDelta::days(1));
    if begin >= end
        || end - begin > TimeDelta::days(RETENTION_DAYS)
        || end > Utc::now() + TimeDelta::minutes(5)
    {
        return Err(ConnectorError::Invalid(format!(
            "Metrics need an ordered time window of at most {RETENTION_DAYS} days."
        )));
    }
    Ok((begin, end, query.limit.unwrap_or(1000).clamp(1, MAX_ROWS)))
}

fn reference_key(reference: &str) -> Option<String> {
    let (connector, metric, location) = reference_keys(reference)?;
    Some(pipe_key(connector, metric, location))
}

fn node_reference(doc: &Doc) -> Option<String> {
    doc_str(doc, "registration_ref").or_else(|| doc_str(doc, "pipe_ref"))
}

fn json_field(doc: &Doc, key: &str) -> serde_json::Value {
    match doc.get(key) {
        Some(meerschaum_kernel::value::Value::Json(value)) => value.clone(),
        _ => doc_str(doc, key)
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default(),
    }
}

fn names_of(run: &Doc, query: &MetricsQuery, current: &HashMap<String, String>) -> Vec<String> {
    let selection = json_field(run, "selection");
    let mut names: Vec<String> = selection
        .get("pipeline_definitions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|definition| {
            definition
                .get("name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| {
                    definition.get("id").and_then(serde_json::Value::as_str)
                        == current.get(name).map(String::as_str)
                })
        })
        .filter_map(|definition| definition.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect();
    // Older run rows only identify a saved Pipeline through the generated run name.
    if names.is_empty()
        && query.visible_pipelines.is_none()
        && let (Some(name), Some(run_id)) = (&query.pipeline, doc_str(run, "run_id"))
        && let Some(rest) = run_id.strip_prefix(&format!("{name}-"))
        && let Some((stamp, suffix)) = rest.rsplit_once('-')
        && suffix.len() == 8
        && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        && chrono::NaiveDateTime::parse_from_str(stamp, "%Y-%m-%dT%H-%M-%S%.6fZ").is_ok()
    {
        names.push(name.clone());
    }
    names.sort();
    names.dedup();
    names
}

async fn engine_docs<C: InstanceConnector + ?Sized>(
    conn: &C,
    metric: &str,
    query: &DataQuery,
) -> ConnectorResult<(Vec<Doc>, bool)> {
    conn.read_engine_metric_rows(metric, query).await
}

/// Read authorized rows with indexed bounds and limits, then aggregate only that visible set.
/// Public transports must derive visibility ceilings themselves, never trust request ceilings.
pub async fn read<C: InstanceConnector + ?Sized>(
    conn: &C,
    query: &MetricsQuery,
) -> ConnectorResult<MetricsResponse> {
    let (begin, end, limit) = query_bounds(query)?;
    let mut response = MetricsResponse {
        begin,
        end,
        limit,
        truncated: false,
        retention_days: RETENTION_DAYS,
        summary: MetricSummary::default(),
        series: Vec::new(),
        pipes: Vec::new(),
        pipelines: Vec::new(),
        history: Vec::new(),
    };
    let visible: Option<HashSet<String>> = query.visible_refs.as_ref().map(|refs| {
        refs.iter()
            .filter_map(|reference| reference_key(reference))
            .collect()
    });
    if visible.as_ref().is_some_and(HashSet::is_empty)
        || query.pipeline.as_ref().is_some_and(|name| {
            query
                .visible_pipelines
                .as_ref()
                .is_some_and(|names| !names.contains(name))
        })
    {
        return Ok(response);
    }
    let selected_key = query
        .pipe_ref
        .as_ref()
        .map(|reference| {
            reference_key(reference).ok_or_else(|| {
                ConnectorError::Invalid("Invalid Pipe reference for metrics.".into())
            })
        })
        .transpose()?;
    if selected_key
        .as_ref()
        .is_some_and(|key| visible.as_ref().is_some_and(|keys| !keys.contains(key)))
    {
        return Ok(response);
    }
    let (current, current_pipelines) = registration_snapshot(conn).await?;
    if current.is_empty() {
        return Ok(response);
    }
    let mut params = Map::new();
    if let Some(key) = &selected_key {
        params.insert("pipe_key".into(), Value::Str(key.clone()));
    }
    let allowed_registrations: Vec<Value> = current
        .iter()
        .filter(|(key, _)| {
            visible
                .as_ref()
                .is_none_or(|visible| visible.contains(*key))
        })
        .filter(|(key, _)| {
            selected_key
                .as_ref()
                .is_none_or(|selected| selected == *key)
        })
        .map(|(_, identity)| Value::Str(identity.clone()))
        .collect();
    if allowed_registrations.is_empty() {
        return Ok(response);
    }
    // Pipeline identities and global run timings are visible only when the whole run is visible.
    let (runs, runs_capped) = engine_docs(
        conn,
        "engine_runs",
        &DataQuery {
            begin: Some(DateBound::utc(end - TimeDelta::days(RETENTION_DAYS))),
            end: None,
            limit: Some(MAX_RUNS as i64),
            order: Some("desc".into()),
            ..Default::default()
        },
    )
    .await?;
    response.truncated |= runs_capped;
    let mut candidate_runs = Vec::new();
    for run in runs {
        if doc_dt(&run, "started").is_none_or(|started| started >= end) {
            continue;
        }
        let names = names_of(&run, query, &current_pipelines);
        let selection = json_field(&run, "selection");
        let snapshots = selection
            .get("pipeline_definitions")
            .and_then(serde_json::Value::as_array);
        if snapshots
            .is_some_and(|snapshots| !snapshots.is_empty() && snapshots.len() != names.len())
            || (query.visible_pipelines.is_some() && snapshots.is_none_or(Vec::is_empty))
        {
            continue;
        }
        if query
            .pipeline
            .as_ref()
            .is_some_and(|name| !names.contains(name))
        {
            continue;
        }
        // A private Pipeline must not label otherwise-visible Pipe metrics or global run totals.
        if query
            .visible_pipelines
            .as_ref()
            .is_some_and(|allowed| names.iter().any(|name| !allowed.contains(name)))
        {
            continue;
        }
        let targets = json_field(&run, "targets");
        let target_refs: Vec<&str> = targets
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .collect();
        if visible.as_ref().is_some_and(|keys| {
            target_refs
                .iter()
                .any(|reference| reference_key(reference).is_none_or(|key| !keys.contains(&key)))
        }) {
            continue;
        }
        candidate_runs.push((run, names));
    }
    let ids: Vec<String> = candidate_runs
        .iter()
        .filter_map(|(run, _)| doc_str(run, "run_id"))
        .collect();
    let mut nodes_by_run: HashMap<String, Vec<Doc>> = HashMap::new();
    if !ids.is_empty() {
        let mut node_params = Map::new();
        node_params.insert(
            "run_id".into(),
            Value::List(ids.iter().cloned().map(Value::Str).collect()),
        );
        let (nodes, nodes_capped) = engine_docs(
            conn,
            "engine_nodes",
            &DataQuery {
                params: Some(node_params),
                limit: Some(MAX_NODES as i64),
                ..Default::default()
            },
        )
        .await?;
        response.truncated |= nodes_capped;
        for node in nodes {
            if let Some(run_id) = doc_str(&node, "run_id") {
                nodes_by_run.entry(run_id).or_default().push(node);
            }
        }
    }
    let mut visible_runs = HashSet::new();
    let mut pipelines: BTreeMap<String, PipelineMetricSummary> = BTreeMap::new();
    for (run, names) in candidate_runs {
        let Some(run_id) = doc_str(&run, "run_id") else {
            continue;
        };
        let nodes = nodes_by_run.remove(&run_id).unwrap_or_default();
        if nodes.len() < doc_u64(&run, "num_nodes") as usize {
            continue;
        }
        if visible.as_ref().is_some_and(|keys| {
            nodes.iter().any(|node| {
                let Some(reference) = node_reference(node) else {
                    return true;
                };
                // A real cross-instance member requires its exact ref in the caller's ceiling.
                let instance = reference.split('|').next().unwrap_or_default();
                if instance != conn.connector_keys() {
                    return query
                        .visible_refs
                        .as_ref()
                        .is_none_or(|refs| !refs.contains(&reference));
                }
                reference_key(&reference).is_none_or(|key| !keys.contains(&key))
            })
        }) {
            continue;
        }
        if nodes.iter().any(|node| {
            let Some(reference) = node_reference(node) else {
                return true;
            };
            let Some(key) = reference_key(&reference) else {
                return true;
            };
            // A legacy node lacks the persisted registration segment, so it cannot prove
            // continuity after the Pipe was deleted or its privacy changed.
            doc_str(node, "registration_id").as_ref() != current.get(&key)
        }) {
            continue;
        }
        visible_runs.insert(run_id.clone());
        let started = doc_dt(&run, "started").unwrap_or(begin);
        let finished = doc_dt(&run, "finished");
        // Pipe outcomes inside the window retain their authorized Pipeline attribution
        // even when the whole run completed outside it. Whole-run timings count only
        // runs completed in the requested window.
        if finished.is_some_and(|finished| finished < begin || finished >= end) {
            continue;
        }
        let mut outcome = SyncOutcome::new(
            doc_bool(&run, "success"),
            "",
            crate::sync_status::SyncSource::Engine,
            None,
        );
        outcome.finished = finished.unwrap_or(started);
        outcome.duration_seconds =
            finished.map(|finished| (finished - started).num_milliseconds().max(0) as f64 / 1000.0);
        outcome.attempts = 0;
        outcome.chunks = 0;
        let mut retries: u64 = 0;
        for node in &nodes {
            outcome.inserted = outcome.inserted.saturating_add(doc_u64(node, "inserted"));
            outcome.updated = outcome.updated.saturating_add(doc_u64(node, "updated"));
            outcome.upserted = outcome.upserted.saturating_add(doc_u64(node, "upserted"));
            outcome.deleted = outcome.deleted.saturating_add(doc_u64(node, "deleted"));
            outcome.chunks = outcome.chunks.saturating_add(doc_u64(node, "num_chunks"));
            outcome.attempts = outcome
                .attempts
                .saturating_add(u32::try_from(doc_u64(node, "attempts")).unwrap_or(u32::MAX));
            retries = retries.saturating_add(doc_u64(node, "attempts").saturating_sub(1));
        }
        let synthetic = Pipe::builder("engine", "run")
            .instance(&conn.connector_keys())
            .build()
            .map_err(|error| ConnectorError::Invalid(error.to_string()))?;
        let metric = SyncMetric::from_outcome(&synthetic, &outcome);
        for name in names {
            if query
                .pipeline
                .as_ref()
                .is_some_and(|selected| selected != &name)
            {
                continue;
            }
            let aggregate =
                pipelines
                    .entry(name.clone())
                    .or_insert_with(|| PipelineMetricSummary {
                        name,
                        ..Default::default()
                    });
            if aggregate.last_started.is_none_or(|last| last < started) {
                aggregate.last_run_id = Some(run_id.clone());
                aggregate.last_started = Some(started);
                aggregate.last_finished = finished;
                aggregate.last_success = finished.map(|_| metric.success);
            }
            if finished.is_some() {
                let previous_retries = aggregate.summary.retries;
                aggregate.summary.add(&metric);
                aggregate.summary.retries = previous_retries.saturating_add(retries);
            } else {
                aggregate.running += 1;
            }
        }
    }
    if query.pipeline.is_some() {
        if visible_runs.is_empty() {
            return Ok(response);
        }
        params.insert(
            "run_id".into(),
            Value::List(visible_runs.iter().cloned().map(Value::Str).collect()),
        );
    }
    let mut history = Vec::new();
    // Each database query is bounded and stays below Oracle's 1,000-value IN limit.
    // Registration stamps already identify both the Pipe and its current ACL segment.
    let filter_batch = if conn.connector_type() == "valkey" {
        // The native candidate cap precedes its in-memory filter; scan that cap once.
        allowed_registrations.len()
    } else {
        900
    };
    for identities in allowed_registrations.chunks(filter_batch) {
        let mut params = params.clone();
        params.insert("registration_id".into(), Value::List(identities.to_vec()));
        let (rows, scan_capped) = conn
            .read_engine_metric_rows(
                METRIC,
                &DataQuery {
                    begin: Some(DateBound::Int(begin.timestamp_micros())),
                    end: Some(DateBound::Int(end.timestamp_micros().saturating_add(1))),
                    params: Some(params),
                    limit: Some((limit + 1) as i64),
                    order: Some("desc".into()),
                    ..Default::default()
                },
            )
            .await?;
        response.truncated |= scan_capped;
        history.extend(rows.iter().filter_map(SyncMetric::from_doc));
        history.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.event_id.cmp(&b.event_id)));
        response.truncated |= history.len() > limit;
        history.truncate(limit + 1);
    }
    // Recheck the ceiling before every aggregation, even if a custom backend ignores params.
    history.retain(|metric| {
        metric.ts >= begin
            && metric.ts < end
            && current.get(&metric.pipe_key) == Some(&metric.registration_id)
            && visible
                .as_ref()
                .is_none_or(|keys| keys.contains(&metric.pipe_key))
            && selected_key
                .as_ref()
                .is_none_or(|key| key == &metric.pipe_key)
            && query.pipeline.as_ref().is_none_or(|_| {
                metric
                    .run_id
                    .as_ref()
                    .is_some_and(|run| visible_runs.contains(run))
            })
    });
    history.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.event_id.cmp(&b.event_id)));
    response.truncated |= history.len() > limit;
    history.truncate(limit);
    let buckets = query.buckets.unwrap_or(48).clamp(1, 200);
    let width = ((end - begin).num_milliseconds().max(1) + buckets as i64 - 1) / buckets as i64;
    let mut series: BTreeMap<i64, MetricSummary> = BTreeMap::new();
    let mut pipes: BTreeMap<String, PipeMetricSummary> = BTreeMap::new();
    for metric in &mut history {
        metric.instance_keys = conn.connector_keys();
        metric.pipe_ref = format!(
            "{}|{}|{}|{}",
            metric.instance_keys,
            metric.connector_keys,
            metric.metric_key,
            metric.location_key.as_deref().unwrap_or("None")
        );
        if !metric
            .run_id
            .as_ref()
            .is_some_and(|run| visible_runs.contains(run))
        {
            metric.run_id = None;
        }
        response.summary.add(metric);
        let bucket = (metric.ts - begin).num_milliseconds() / width;
        series.entry(bucket).or_default().add(metric);
        pipes
            .entry(metric.pipe_key.clone())
            .or_insert_with(|| PipeMetricSummary {
                pipe_ref: metric.pipe_ref.clone(),
                connector_keys: metric.connector_keys.clone(),
                metric_key: metric.metric_key.clone(),
                location_key: metric.location_key.clone(),
                summary: MetricSummary::default(),
            })
            .summary
            .add(metric);
    }
    response.series = series
        .into_iter()
        .map(|(bucket, summary)| MetricBucket {
            ts: begin + TimeDelta::milliseconds(bucket * width),
            summary,
        })
        .collect();
    response.pipes = pipes.into_values().collect();
    response.pipelines = pipelines.into_values().collect();
    response.history = history;
    Ok(response)
}
