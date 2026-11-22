//! Bounded, numeric sync history shared by ordinary syncs, engine runs, SQL and Valkey.
//! Recording never waits on a database; persisted rows survive process and API worker restarts.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use serde::{Deserialize, Serialize};

use crate::pipe_ops::Doc;
use crate::sync_status::{
    SyncOutcome, doc_bool, doc_dt, doc_f64, doc_str, doc_u64, static_context,
};
use crate::{ConnectorError, ConnectorResult, InstanceConnector, SyncOptions};

mod query;
mod recorder;
mod registration;

use query::query_bounds;
pub use query::read;
pub use recorder::{flush, has_unwritten, prune, record};
pub use registration::{
    observed_registration, pipe_key, reference_keys, registration_matches, registration_stamp,
    registrations, snapshot_registry, strip_snapshot_proofs,
};

/// The internal history pipe, placed by the instance beside its other engine state.
pub const METRIC: &str = "engine_metrics";
/// Persisted history expires after this many days.
pub const RETENTION_DAYS: i64 = 30;
/// Maximum outcomes waiting in this process; overload drops telemetry, never the sync.
pub const MAX_PENDING: usize = 4096;
/// Largest database write batch.
pub const BATCH_SIZE: usize = 128;
/// Maximum detailed sync rows returned by one request.
pub const MAX_ROWS: usize = 5000;
/// Maximum database-aggregated Pipe/time groups transferred by one request.
pub const MAX_SUMMARY_GROUPS: usize = 20_000;
/// Maximum run rows considered by one request.
pub const MAX_RUNS: usize = 1000;
/// Maximum node rows considered by one request.
pub const MAX_NODES: usize = 10000;
/// Database time budget of one background write.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(3);

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
            started: outcome
                .finished
                .checked_sub_signed(TimeDelta::microseconds((duration * 1_000_000.0) as i64))
                .unwrap_or(DateTime::<Utc>::MIN_UTC),
            pipe_key: pipe_key(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
            ),
            registration_id: if outcome.registration_captured {
                outcome.registration_id.clone()
            } else {
                observed_registration(pipe)
            }
            .unwrap_or_default(),
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
        self.started =
            self.ts - TimeDelta::microseconds((self.duration_seconds * 1_000_000.0) as i64);
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
        // MySQL DATETIME(0) may round a completed sync into the following second.
        // The indexed integer is the captured time, independent of physical datetime precision.
        let ts = doc_str(doc, "ts_us")
            .and_then(|value| value.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_micros)
            .or_else(|| doc_dt(doc, "ts"))?;
        let duration_seconds = doc_f64(doc, "duration_seconds")
            .filter(|value| {
                value.is_finite() && *value >= 0.0 && *value <= RETENTION_DAYS as f64 * 86400.0
            })
            .unwrap_or(0.0);
        let started = ts.checked_sub_signed(TimeDelta::microseconds(
            (duration_seconds * 1_000_000.0) as i64,
        ))?;
        Some(Self {
            event_id: doc_str(doc, "event_id")?,
            ts,
            started,
            pipe_key: doc_str(doc, "pipe_key")?,
            registration_id: doc_str(doc, "registration_id")?,
            pipe_ref: doc_str(doc, "pipe_ref")?,
            instance_keys: doc_str(doc, "instance_keys")?,
            connector_keys: doc_str(doc, "connector_keys")?,
            metric_key: doc_str(doc, "metric_key")?,
            location_key: doc_str(doc, "location_key"),
            run_id: doc_str(doc, "run_id"),
            success: doc_bool(doc, "success"),
            duration_seconds,
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
    /// Omit individual outcomes and Pipeline groups (for summary-only dashboard cards).
    #[serde(default)]
    pub summary_only: bool,
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
    /// Merge already-authorized aggregates, weighting averages by completed syncs.
    pub fn merge(&mut self, other: &Self) {
        self.syncs = self.syncs.saturating_add(other.syncs);
        self.succeeded = self.succeeded.saturating_add(other.succeeded);
        self.failed = self.failed.saturating_add(other.failed);
        self.inserted = self.inserted.saturating_add(other.inserted);
        self.updated = self.updated.saturating_add(other.updated);
        self.upserted = self.upserted.saturating_add(other.upserted);
        self.deleted = self.deleted.saturating_add(other.deleted);
        self.rows = self
            .inserted
            .saturating_add(self.updated)
            .saturating_add(self.upserted)
            .saturating_add(self.deleted);
        self.attempts = self.attempts.saturating_add(other.attempts);
        self.retries = self.retries.saturating_add(other.retries);
        self.chunks = self.chunks.saturating_add(other.chunks);
        self.duration_seconds += other.duration_seconds;
        self.success_rate = (self.syncs > 0).then(|| self.succeeded as f64 / self.syncs as f64);
        self.avg_duration_seconds =
            (self.syncs > 0).then(|| self.duration_seconds / self.syncs as f64);
        self.rows_per_second =
            (self.duration_seconds > 0.0).then(|| self.rows as f64 / self.duration_seconds);
        self.last_sync = self.last_sync.max(other.last_sync);
        self.last_success = self.last_success.max(other.last_success);
        self.last_failure = self.last_failure.max(other.last_failure);
    }

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

/// One database-aggregated registration/time group, before the reader rechecks visibility.
#[derive(Debug, Clone)]
pub struct MetricAggregate {
    /// Stable Pipe key within the instance.
    pub pipe_key: String,
    /// Exact current registration/ACL segment.
    pub registration_id: String,
    /// Zero-based bucket within the requested window.
    pub bucket: usize,
    /// Numeric results for this group.
    pub summary: MetricSummary,
}

impl MetricAggregate {
    /// Decode the fixed aggregate projection returned by a native backend.
    pub fn from_doc(doc: &Doc) -> Option<Self> {
        let mut summary = MetricSummary {
            syncs: doc_u64(doc, "syncs"),
            succeeded: doc_u64(doc, "succeeded"),
            failed: doc_u64(doc, "failed"),
            inserted: doc_u64(doc, "inserted"),
            updated: doc_u64(doc, "updated"),
            upserted: doc_u64(doc, "upserted"),
            deleted: doc_u64(doc, "deleted"),
            attempts: doc_u64(doc, "attempts"),
            retries: doc_u64(doc, "retries"),
            chunks: doc_u64(doc, "chunks"),
            duration_seconds: doc_f64(doc, "duration_seconds")?,
            last_sync: doc_str(doc, "last_sync_us")
                .and_then(|s| s.parse().ok())
                .and_then(DateTime::from_timestamp_micros),
            last_success: doc_str(doc, "last_success_us")
                .and_then(|s| s.parse().ok())
                .and_then(DateTime::from_timestamp_micros),
            last_failure: doc_str(doc, "last_failure_us")
                .and_then(|s| s.parse().ok())
                .and_then(DateTime::from_timestamp_micros),
            ..Default::default()
        };
        if !summary.duration_seconds.is_finite() || summary.duration_seconds < 0.0 {
            return None;
        }
        summary.merge(&MetricSummary::default());
        Some(Self {
            pipe_key: doc_str(doc, "pipe_key")?,
            registration_id: doc_str(doc, "registration_id")?,
            bucket: usize::try_from(doc_u64(doc, "bucket")).ok()?,
            summary,
        })
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
    /// Totals and charts cover every authorized sync in the window, independently of history.
    #[serde(default)]
    pub summary_complete: bool,
    /// More individual syncs match than the detailed history contains.
    #[serde(default)]
    pub history_truncated: bool,
    /// Run/node limits prevent complete Pipeline run totals.
    #[serde(default)]
    pub pipelines_truncated: bool,
    /// Applied number of chart buckets.
    #[serde(default = "default_bucket_count")]
    pub buckets: usize,
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

fn default_bucket_count() -> usize {
    48
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
        let Some(mut pipe) = conn
            .engine_state_pipe(&conn.connector_keys(), metric)
            .await?
        else {
            continue;
        };
        if metric == "engine_nodes" {
            // Match InstanceState's node writes: old installations need the newly
            // declared registration columns, without assigning identity to legacy rows.
            pipe.update_parameters([("static".into(), Value::Bool(false))].into());
        }
        // Use the writer's cold-readiness path even when the table already exists:
        // existence alone does not prove that a concurrent writer finished its indexes.
        // In particular, this keeps PostgreSQL's initialization lock around all DDL.
        let frame = pipe_enforce_dtypes(PipeInput::Docs(Vec::new()), &static_context(&pipe))
            .map_err(|error| ConnectorError::Invalid(error.to_string()))?;
        let result = conn.sync_pipe(&pipe, frame, &write_options()).await;
        if !result.success {
            return Err(ConnectorError::Backend(result.message));
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

#[cfg(test)]
mod bounds_tests;
