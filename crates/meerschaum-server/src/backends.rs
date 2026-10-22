//! The pluggable backends behind the routes that are not pipe operations: action dispatch
//! (`/actions`), jobs (`/jobs/*`, `/logs/*`), engine state (`/engine/*`, §5.8) and extra
//! instances (`instance_keys=`).
//!
//! The server owns the HTTP contract; these traits own the behavior. Each has a default that
//! answers like a v4 server with nothing installed (no actions, no jobs) or an in-memory store
//! (engine state), so the server runs and is testable before `meerschaum-jobs`, the CLI action
//! table and the instance-backed engine state land.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector::pipe_ops::Doc;
use meerschaum_connector_api::engine_pipes::EnginePipeQuery;
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineState, LedgerEntry, RunRecord,
};
use meerschaum_core::{Map, SuccessTuple};
use meerschaum_kernel::Frame;
use serde_json::Value as Json;
use tokio::sync::mpsc;

// ------------------------------------------------------------------------------------------------
// Actions
// ------------------------------------------------------------------------------------------------

/// Action dispatch for `/actions` (`meerschaum.actions.actions`).
#[async_trait]
pub trait ActionsBackend: Send + Sync {
    /// `list(actions)`, in the action table's order.
    fn list(&self) -> Vec<String>;

    /// Run `actions[action](**keywords)`. `keywords` already carries `mrsm_instance` and
    /// `debug`.
    async fn execute(&self, action: &str, keywords: Map) -> SuccessTuple;
}

/// No actions installed: every action is invalid.
#[derive(Debug, Default, Clone)]
pub struct NoActions;

#[async_trait]
impl ActionsBackend for NoActions {
    fn list(&self) -> Vec<String> {
        Vec::new()
    }

    async fn execute(&self, action: &str, _keywords: Map) -> SuccessTuple {
        SuccessTuple::fail(format!("Invalid action '{action}'."))
    }
}

// ------------------------------------------------------------------------------------------------
// Jobs
// ------------------------------------------------------------------------------------------------

/// One event of a job's log stream (`Job.monitor_logs_async` callbacks).
#[derive(Debug, Clone, PartialEq)]
pub enum LogEvent {
    /// A chunk of log text (`callback_function`).
    Line(String),
    /// The job is blocking on stdin (`input_callback_function`); the next client message is
    /// its input.
    StdinRequested,
    /// The job stopped (`stop_callback_function`) with its result.
    Stopped(SuccessTuple),
}

/// A live log monitor: events out, stdin in.
#[derive(Debug)]
pub struct LogMonitor {
    /// The log events.
    pub events: mpsc::Receiver<LogEvent>,
    /// Lines typed by clients, forwarded to the job's stdin.
    pub input: mpsc::Sender<String>,
}

/// The job outcome of `POST /jobs/{name}`.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateJob {
    /// The job was created; the start result.
    Started(SuccessTuple),
    /// The job already exists (409 `"{job} already exists."`).
    Exists,
}

/// Jobs for `/jobs/*` and `/logs/*` (`meerschaum.jobs.Job` with the `local` executor, falling
/// back from `systemd`).
#[async_trait]
pub trait JobsBackend: Send + Sync {
    /// `str(Job(name))` (`Job("name", "sysargs")`), used in error details.
    fn job_str(&self, name: &str) -> String {
        format!("Job(\"{name}\")")
    }

    /// Every job's metadata (`GET /jobs`): `{name: {sysargs, result, restart, status, daemon}}`.
    async fn list(&self) -> IndexMap<String, Json>;

    /// One job's metadata, or `None` when the job does not exist.
    async fn get(&self, name: &str) -> Option<Json>;

    /// `job.exists()`.
    async fn exists(&self, name: &str) -> bool;

    /// Create and start a job with cleaned `sysargs` and optional properties.
    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob;

    /// Promote an automatic execution to a persistent user-managed Job.
    async fn promote(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail("This jobs backend does not support promotion.")
    }

    /// `job.delete()`.
    async fn delete(&self, name: &str) -> SuccessTuple;

    /// `job.start()`.
    async fn start(&self, name: &str) -> SuccessTuple;

    /// `job.stop()`.
    async fn stop(&self, name: &str) -> SuccessTuple;

    /// `job.pause()`.
    async fn pause(&self, name: &str) -> SuccessTuple;

    /// `job.get_logs()`.
    async fn logs(&self, name: &str) -> Option<String>;

    /// `job.stop_time`.
    async fn stop_time(&self, name: &str) -> Option<DateTime<Utc>>;

    /// Natural completion timestamp, falling back to a manual stop for legacy backends.
    async fn completion_time(&self, name: &str) -> Option<DateTime<Utc>> {
        self.stop_time(name).await
    }

    /// `job.is_blocking_on_stdin()`.
    async fn is_blocking_on_stdin(&self, name: &str) -> bool;

    /// `job.get_prompt_kwargs()`.
    async fn prompt_kwargs(&self, name: &str) -> Map;

    /// Start monitoring a job's logs (`None` when it cannot be monitored).
    async fn monitor_logs(&self, name: &str) -> Option<LogMonitor>;
}

/// No jobs: nothing exists, and creating or starting one fails.
#[derive(Debug, Default, Clone)]
pub struct NoJobs;

const NO_JOBS: &str = "Jobs are not available on this server.";

#[async_trait]
impl JobsBackend for NoJobs {
    async fn list(&self) -> IndexMap<String, Json> {
        IndexMap::new()
    }
    async fn get(&self, _name: &str) -> Option<Json> {
        None
    }
    async fn exists(&self, _name: &str) -> bool {
        false
    }
    async fn create(&self, _: &str, _: Vec<String>, _: Option<Map>) -> CreateJob {
        CreateJob::Started(SuccessTuple::fail(NO_JOBS))
    }
    async fn delete(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(NO_JOBS)
    }
    async fn start(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(NO_JOBS)
    }
    async fn stop(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(NO_JOBS)
    }
    async fn pause(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(NO_JOBS)
    }
    async fn logs(&self, _name: &str) -> Option<String> {
        None
    }
    async fn stop_time(&self, _name: &str) -> Option<DateTime<Utc>> {
        None
    }
    async fn is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    async fn prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
    async fn monitor_logs(&self, _name: &str) -> Option<LogMonitor> {
        None
    }
}

// ------------------------------------------------------------------------------------------------
// Engine state (§5.8)
// ------------------------------------------------------------------------------------------------

/// Engine state for the `/engine/*` routes. The shapes are `meerschaum_connector_api::models`.
#[async_trait]
pub trait EngineBackend: Send + Sync {
    /// `POST /engine/ledger`.
    async fn append_ledger(&self, entries: Vec<LedgerEntry>) -> SuccessTuple;
    /// `GET /engine/ledger?child_ref=&after_seq=`: the child's parents' entries.
    async fn ledger_after(&self, child_ref: &str, after_seq: Option<i64>) -> Vec<LedgerEntry>;
    /// `PUT /engine/cursors`.
    async fn put_cursors(&self, cursors: Vec<EdgeCursor>) -> SuccessTuple;
    /// `GET /engine/state?pipe_ref=`.
    async fn get_state(&self, pipe_ref: &str) -> Option<EngineState>;
    /// `PUT /engine/state`.
    async fn put_states(&self, states: Vec<EngineState>) -> SuccessTuple;
    /// `POST /engine/runs`.
    async fn record_runs(&self, runs: Vec<RunRecord>) -> SuccessTuple;
    /// `GET /engine/runs?run_id=&limit=` (newest first).
    async fn get_runs(&self, run_id: Option<&str>, limit: Option<i64>) -> Vec<RunRecord>;
    /// `POST /engine/events`.
    async fn record_events(&self, events: Vec<EngineEvent>) -> SuccessTuple;
    /// `POST /engine/leases`: acquire or extend.
    async fn acquire_lease(&self, pipe_ref: &str, holder: &str, ttl: Duration) -> bool;
    /// `DELETE /engine/leases`.
    async fn release_lease(&self, pipe_ref: &str, holder: &str) -> bool;

    /// `POST /engine/pipes/{metric}/read`: rows of an engine state pipe (`None`: none).
    /// The default (no instance behind the store) is [`EngineStoreError::Unavailable`].
    async fn read_pipe(
        &self,
        metric: &str,
        query: &EnginePipeQuery,
    ) -> Result<Option<Frame>, EngineStoreError> {
        let _ = (metric, query);
        Err(EngineStoreError::Unavailable)
    }
    /// `POST /engine/pipes/{metric}/write`: append (upsert for the upsert pipes) rows.
    async fn write_pipe(
        &self,
        metric: &str,
        docs: Vec<Doc>,
    ) -> Result<SuccessTuple, EngineStoreError> {
        let _ = (metric, docs);
        Err(EngineStoreError::Unavailable)
    }
    /// `POST /engine/pipes/{metric}/clear`: delete the rows matching the query.
    async fn clear_pipe(
        &self,
        metric: &str,
        query: &EnginePipeQuery,
    ) -> Result<SuccessTuple, EngineStoreError> {
        let _ = (metric, query);
        Err(EngineStoreError::Unavailable)
    }
    /// `GET /engine/pipes/{metric}/exists`.
    async fn pipe_exists(&self, metric: &str) -> Result<bool, EngineStoreError> {
        let _ = metric;
        Err(EngineStoreError::Unavailable)
    }
}

/// Why an engine-pipe primitive could not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineStoreError {
    /// The store keeps no state pipes (the in-memory backend): `501`.
    Unavailable,
    /// The instance failed: `500` with the message.
    Failed(String),
}

impl std::fmt::Display for EngineStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineStoreError::Unavailable => f.write_str(
                "This server keeps no engine state pipes (no instance-backed engine store).",
            ),
            EngineStoreError::Failed(m) => f.write_str(m),
        }
    }
}

#[derive(Debug, Default)]
struct MemoryEngineData {
    ledger: Vec<LedgerEntry>,
    cursors: IndexMap<(String, String), EdgeCursor>,
    states: IndexMap<String, EngineState>,
    runs: Vec<RunRecord>,
    events: Vec<EngineEvent>,
    leases: HashMap<String, (String, Instant)>,
}

/// An in-memory [`EngineBackend`] (tests and builds without an instance; `start api` uses
/// [`crate::engine_store::InstanceEngine`]). A child's parents are the parents of its edge
/// cursors. It keeps no state pipes, so the `/engine/pipes/*` primitives answer `501`.
#[derive(Debug, Default)]
pub struct MemoryEngine {
    data: Mutex<MemoryEngineData>,
}

impl MemoryEngine {
    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryEngineData> {
        self.data.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The number of recorded events (for tests).
    pub fn num_events(&self) -> usize {
        self.lock().events.len()
    }
}

#[async_trait]
impl EngineBackend for MemoryEngine {
    async fn append_ledger(&self, entries: Vec<LedgerEntry>) -> SuccessTuple {
        let n = entries.len();
        self.lock().ledger.extend(entries);
        SuccessTuple::ok(format!("Appended {n} ledger entries."))
    }

    async fn ledger_after(&self, child_ref: &str, after_seq: Option<i64>) -> Vec<LedgerEntry> {
        let data = self.lock();
        let parents: Vec<&String> = data
            .cursors
            .keys()
            .filter(|(_, c)| c == child_ref)
            .map(|(p, _)| p)
            .collect();
        data.ledger
            .iter()
            .filter(|e| parents.contains(&&e.pipe_ref))
            .filter(|e| after_seq.is_none_or(|s| e.seq > s))
            .cloned()
            .collect()
    }

    async fn put_cursors(&self, cursors: Vec<EdgeCursor>) -> SuccessTuple {
        let mut data = self.lock();
        let n = cursors.len();
        for c in cursors {
            data.cursors
                .insert((c.parent_ref.clone(), c.child_ref.clone()), c);
        }
        SuccessTuple::ok(format!("Upserted {n} cursors."))
    }

    async fn get_state(&self, pipe_ref: &str) -> Option<EngineState> {
        self.lock().states.get(pipe_ref).cloned()
    }

    async fn put_states(&self, states: Vec<EngineState>) -> SuccessTuple {
        let mut data = self.lock();
        let n = states.len();
        for s in states {
            data.states.insert(s.pipe_ref.clone(), s);
        }
        SuccessTuple::ok(format!("Upserted {n} states."))
    }

    async fn record_runs(&self, runs: Vec<RunRecord>) -> SuccessTuple {
        let n = runs.len();
        self.lock().runs.extend(runs);
        SuccessTuple::ok(format!("Recorded {n} runs."))
    }

    async fn get_runs(&self, run_id: Option<&str>, limit: Option<i64>) -> Vec<RunRecord> {
        let data = self.lock();
        let mut runs: Vec<RunRecord> = data
            .runs
            .iter()
            .filter(|r| run_id.is_none_or(|id| r.run_id == id))
            .cloned()
            .collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.started));
        if let Some(limit) = limit {
            runs.truncate(usize::try_from(limit.max(0)).unwrap_or(0));
        }
        runs
    }

    async fn record_events(&self, events: Vec<EngineEvent>) -> SuccessTuple {
        let n = events.len();
        self.lock().events.extend(events);
        SuccessTuple::ok(format!("Recorded {n} events."))
    }

    async fn acquire_lease(&self, pipe_ref: &str, holder: &str, ttl: Duration) -> bool {
        let mut data = self.lock();
        let now = Instant::now();
        match data.leases.get(pipe_ref) {
            Some((h, expires)) if h != holder && *expires > now => false,
            _ => {
                data.leases
                    .insert(pipe_ref.to_string(), (holder.to_string(), now + ttl));
                true
            }
        }
    }

    async fn release_lease(&self, pipe_ref: &str, holder: &str) -> bool {
        let mut data = self.lock();
        match data.leases.get(pipe_ref) {
            Some((h, _)) if h == holder => {
                data.leases.remove(pipe_ref);
                true
            }
            _ => false,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Extra instances
// ------------------------------------------------------------------------------------------------

/// Resolves `instance_keys=` other than the server's own instance (`parse_instance_keys`).
pub trait InstanceResolver: Send + Sync {
    /// The connector for `keys`, or `None` (422 `Invalid instance keys.`).
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>>;
}

/// Resolves nothing: only the server's own instance is served.
#[derive(Debug, Default, Clone)]
pub struct NoInstances;

impl InstanceResolver for NoInstances {
    fn resolve(&self, _keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_leases() {
        let e = MemoryEngine::default();
        assert!(e.acquire_lease("p", "a", Duration::from_secs(60)).await);
        assert!(!e.acquire_lease("p", "b", Duration::from_secs(60)).await);
        assert!(e.acquire_lease("p", "a", Duration::from_secs(60)).await);
        assert!(!e.release_lease("p", "b").await);
        assert!(e.release_lease("p", "a").await);
        assert!(e.acquire_lease("p", "b", Duration::from_millis(0)).await);
        assert!(e.acquire_lease("p", "a", Duration::from_secs(1)).await);
    }
}
