//! Engine state access (§5.3, §5.8) behind traits.
//!
//! The planner reads through a [`StateSource`]; the executor writes through a [`StateWriter`].
//! The instance-backed implementations (the `mrsm:engine_*` internal pipes, the `/engine` API
//! routes) arrive with the instance connectors. [`MemoryState`] implements both in memory for
//! tests and golden fixtures, and [`CachedState`] caches watermarks for the duration of a run.
//!
//! Every read and write of ledger entries and cursors names the **state instance** that holds
//! them: the child's instance for the edge (§5.1). Watermarks and definition hashes live on the
//! pipe's own instance.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use meerschaum_core::success::Bound;
use serde::{Deserialize, Serialize};

use crate::ledger::{EdgeCursor, LedgerEntry};
use crate::node::PipeRef;
use crate::runs::RunRecord;
use crate::status::{SyncOutcome, SyncStatus};

/// A failure reading or writing engine state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// The instance could not be reached. A parent on an unreachable instance makes its edge
    /// `upstream_failed` (§5.1).
    #[error("instance '{instance}' is unreachable: {message}")]
    Unreachable {
        /// The instance keys.
        instance: String,
        /// Details.
        message: String,
    },
    /// Engine state cannot be written (read-only instance, §5.8).
    #[error("engine state on instance '{instance}' is read-only")]
    ReadOnly {
        /// The instance keys.
        instance: String,
    },
    /// Any other failure.
    #[error("{0}")]
    Other(String),
}

/// Read access to engine state, used by the planner.
pub trait StateSource: Send + Sync {
    /// The pipe's watermark (`get_sync_time()`), read through the pipe's own instance.
    /// `Ok(None)` for an empty pipe or a pipe without a datetime column.
    fn watermark(
        &self,
        node: &PipeRef,
    ) -> impl Future<Output = Result<Option<Bound>, StateError>> + Send;

    /// The stored definition hash (`mrsm:engine_state`), if any.
    fn definition_hash(
        &self,
        node: &PipeRef,
    ) -> impl Future<Output = Result<Option<String>, StateError>> + Send;

    /// The edge cursor stored on `state_instance`.
    fn cursor(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> impl Future<Output = Result<Option<EdgeCursor>, StateError>> + Send;

    /// The parent's ledger entries on `state_instance` with `seq > after_seq`, in `seq` order.
    fn ledger_after(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        after_seq: i64,
    ) -> impl Future<Output = Result<Vec<LedgerEntry>, StateError>> + Send;

    /// The parent's highest sequence number on `state_instance`.
    fn latest_seq(
        &self,
        state_instance: &str,
        parent: &PipeRef,
    ) -> impl Future<Output = Result<Option<i64>, StateError>> + Send;
}

/// Write access to engine state, used by the executor (and by syncs writing the ledger).
pub trait StateWriter: Send + Sync {
    /// Upsert an edge cursor on `state_instance` (after the child's sync succeeded).
    fn put_cursor(
        &self,
        state_instance: &str,
        cursor: EdgeCursor,
    ) -> impl Future<Output = Result<(), StateError>> + Send;

    /// Upsert several cursors on one state instance. Implementations write them in one
    /// statement where they can (the engine-state cost target, §5.8); the default writes them
    /// one at a time.
    fn put_cursors(
        &self,
        state_instance: &str,
        cursors: Vec<EdgeCursor>,
    ) -> impl Future<Output = Result<(), StateError>> + Send {
        async move {
            for c in cursors {
                self.put_cursor(state_instance, c).await?;
            }
            Ok(())
        }
    }

    /// Upsert a pipe's definition hash.
    fn put_definition_hash(
        &self,
        node: &PipeRef,
        hash: &str,
    ) -> impl Future<Output = Result<(), StateError>> + Send;

    /// Append ledger entries for `pipe_ref` on `state_instance`; the instance assigns the
    /// sequence numbers. Returns the stored entries.
    fn append_ledger(
        &self,
        state_instance: &str,
        pipe_ref: &PipeRef,
        spans: Vec<(Option<Bound>, Option<Bound>)>,
    ) -> impl Future<Output = Result<Vec<LedgerEntry>, StateError>> + Send;

    /// Delete the parent's entries with `seq <= through_seq` on `state_instance` (compaction).
    fn prune_ledger(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        through_seq: i64,
    ) -> impl Future<Output = Result<usize, StateError>> + Send;

    /// Upsert a run and the supplied node rows; omitted nodes remain unchanged.
    fn record_run(&self, run: &RunRecord) -> impl Future<Output = Result<(), StateError>> + Send;

    /// Record a node's final sync outcome (after every window and retry) in its pipe's sync
    /// status (rc15 contract C2, [`crate::status`]). Best effort and immediate: an
    /// implementation that writes to an instance hands the write to the background (see
    /// [`crate::InstanceState::flush_sync_statuses`]). The default records nothing.
    fn record_sync_status(&self, node: &PipeRef, outcome: SyncOutcome) {
        let _ = (node, outcome);
    }
}

/// The serializable contents of a [`MemoryState`] (the `state` block of a golden fixture).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MemoryStateData {
    /// Watermarks by node; a missing node has no watermark.
    #[serde(default)]
    pub watermarks: BTreeMap<PipeRef, Option<Bound>>,
    /// Stored definition hashes by node.
    #[serde(default)]
    pub definition_hashes: BTreeMap<PipeRef, String>,
    /// Ledger entries by state instance.
    #[serde(default)]
    pub ledger: BTreeMap<String, Vec<LedgerEntry>>,
    /// Edge cursors by state instance.
    #[serde(default)]
    pub cursors: BTreeMap<String, Vec<EdgeCursor>>,
    /// Instances whose reads and writes fail with [`StateError::Unreachable`].
    #[serde(default)]
    pub unreachable_instances: Vec<String>,
    /// Recorded runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<RunRecord>,
    /// Recorded sync statuses by node ([`StateWriter::record_sync_status`]).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sync_statuses: BTreeMap<PipeRef, SyncStatus>,
}

/// An in-memory [`StateSource`] + [`StateWriter`].
#[derive(Debug, Default)]
pub struct MemoryState {
    inner: Mutex<MemoryStateData>,
}

impl MemoryState {
    /// Wrap state data.
    pub fn new(data: MemoryStateData) -> MemoryState {
        MemoryState {
            inner: Mutex::new(data),
        }
    }

    /// A copy of the current data.
    pub fn snapshot(&self) -> MemoryStateData {
        self.lock().clone()
    }

    /// Set a node's watermark.
    pub fn set_watermark(&self, node: &PipeRef, watermark: Option<Bound>) {
        self.lock().watermarks.insert(node.clone(), watermark);
    }

    /// Mark an instance reachable or unreachable.
    pub fn set_unreachable(&self, instance: &str, unreachable: bool) {
        let mut data = self.lock();
        data.unreachable_instances.retain(|i| i != instance);
        if unreachable {
            data.unreachable_instances.push(instance.to_string());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryStateData> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn check(
        &self,
        instance: &str,
    ) -> Result<std::sync::MutexGuard<'_, MemoryStateData>, StateError> {
        let data = self.lock();
        if data.unreachable_instances.iter().any(|i| i == instance) {
            return Err(StateError::Unreachable {
                instance: instance.to_string(),
                message: "connection refused".into(),
            });
        }
        Ok(data)
    }
}

impl StateSource for MemoryState {
    async fn watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        let data = self.check(node.instance_keys())?;
        Ok(data.watermarks.get(node).cloned().flatten())
    }

    async fn definition_hash(&self, node: &PipeRef) -> Result<Option<String>, StateError> {
        let data = self.check(node.instance_keys())?;
        Ok(data.definition_hashes.get(node).cloned())
    }

    async fn cursor(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> Result<Option<EdgeCursor>, StateError> {
        let data = self.check(state_instance)?;
        Ok(data.cursors.get(state_instance).and_then(|cs| {
            cs.iter()
                .find(|c| c.parent_ref == *parent && c.child_ref == *child)
                .cloned()
        }))
    }

    async fn ledger_after(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        after_seq: i64,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        let data = self.check(state_instance)?;
        let mut out: Vec<LedgerEntry> = data
            .ledger
            .get(state_instance)
            .into_iter()
            .flatten()
            .filter(|e| e.pipe_ref == *parent && e.seq > after_seq)
            .cloned()
            .collect();
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    async fn latest_seq(
        &self,
        state_instance: &str,
        parent: &PipeRef,
    ) -> Result<Option<i64>, StateError> {
        let data = self.check(state_instance)?;
        Ok(data
            .ledger
            .get(state_instance)
            .into_iter()
            .flatten()
            .filter(|e| e.pipe_ref == *parent)
            .map(|e| e.seq)
            .max())
    }
}

impl StateWriter for MemoryState {
    async fn put_cursor(&self, state_instance: &str, cursor: EdgeCursor) -> Result<(), StateError> {
        let mut data = self.check(state_instance)?;
        let cursors = data.cursors.entry(state_instance.to_string()).or_default();
        match cursors
            .iter_mut()
            .find(|c| c.parent_ref == cursor.parent_ref && c.child_ref == cursor.child_ref)
        {
            Some(existing) => *existing = cursor,
            None => cursors.push(cursor),
        }
        Ok(())
    }

    async fn put_definition_hash(&self, node: &PipeRef, hash: &str) -> Result<(), StateError> {
        let mut data = self.check(node.instance_keys())?;
        data.definition_hashes
            .insert(node.clone(), hash.to_string());
        Ok(())
    }

    async fn append_ledger(
        &self,
        state_instance: &str,
        pipe_ref: &PipeRef,
        spans: Vec<(Option<Bound>, Option<Bound>)>,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        let mut data = self.check(state_instance)?;
        let entries = data.ledger.entry(state_instance.to_string()).or_default();
        let last = entries
            .iter()
            .filter(|e| e.pipe_ref == *pipe_ref)
            .map(|e| e.seq)
            .max()
            .unwrap_or(0);
        let mut out = Vec::with_capacity(spans.len());
        for (seq, (begin, end)) in (last + 1..).zip(spans) {
            let entry = LedgerEntry {
                pipe_ref: pipe_ref.clone(),
                seq,
                begin,
                end,
                created: Utc::now(),
            };
            entries.push(entry.clone());
            out.push(entry);
        }
        Ok(out)
    }

    async fn prune_ledger(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        through_seq: i64,
    ) -> Result<usize, StateError> {
        let mut data = self.check(state_instance)?;
        let entries = data.ledger.entry(state_instance.to_string()).or_default();
        let before = entries.len();
        // Keep the newest entry: it holds the sequence high-water mark (see `InstanceState`).
        let newest = entries
            .iter()
            .filter(|e| e.pipe_ref == *parent)
            .map(|e| e.seq)
            .max();
        entries
            .retain(|e| !(e.pipe_ref == *parent && e.seq <= through_seq && Some(e.seq) != newest));
        Ok(before - entries.len())
    }

    async fn record_run(&self, run: &RunRecord) -> Result<(), StateError> {
        let mut data = self.lock();
        if let Some(existing) = data
            .runs
            .iter_mut()
            .find(|r| r.run.run_id == run.run.run_id)
        {
            existing.run = run.run.clone();
            for row in &run.nodes {
                if let Some(stored) = existing
                    .nodes
                    .iter_mut()
                    .find(|n| n.pipe_ref == row.pipe_ref)
                {
                    *stored = row.clone();
                } else {
                    existing.nodes.push(row.clone());
                }
            }
        } else {
            data.runs.push(run.clone());
        }
        Ok(())
    }

    fn record_sync_status(&self, node: &PipeRef, outcome: SyncOutcome) {
        let mut data = self.lock();
        let status = match data.sync_statuses.remove(node) {
            Some(status) => status,
            None => {
                let parts = node.parts();
                match meerschaum_core::Pipe::builder(parts.connector_keys, parts.metric_key)
                    .location(parts.location_key)
                    .instance(parts.instance_keys)
                    .build()
                {
                    Ok(pipe) => SyncStatus::for_pipe(&pipe),
                    Err(_) => return,
                }
            }
        };
        data.sync_statuses
            .insert(node.clone(), status.apply(&outcome));
    }
}

/// A [`StateSource`] that caches watermarks for the duration of a run (§5.3: watermarks are
/// derived on demand and cached in memory). Call [`CachedState::invalidate`] after a node syncs.
#[derive(Debug)]
pub struct CachedState<'a, S> {
    inner: &'a S,
    watermarks: Mutex<HashMap<PipeRef, Result<Option<Bound>, StateError>>>,
}

impl<'a, S: StateSource> CachedState<'a, S> {
    /// Wrap a state source.
    pub fn new(inner: &'a S) -> CachedState<'a, S> {
        CachedState {
            inner,
            watermarks: Mutex::new(HashMap::new()),
        }
    }

    /// Forget a node's cached watermark.
    pub fn invalidate(&self, node: &PipeRef) {
        self.watermarks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(node);
    }

    /// The wrapped source.
    pub fn inner(&self) -> &'a S {
        self.inner
    }
}

impl<S: StateSource> StateSource for CachedState<'_, S> {
    async fn watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        let cached = self
            .watermarks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(node)
            .cloned();
        if let Some(v) = cached {
            return v;
        }
        let v = self.inner.watermark(node).await;
        self.watermarks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node.clone(), v.clone());
        v
    }

    async fn definition_hash(&self, node: &PipeRef) -> Result<Option<String>, StateError> {
        self.inner.definition_hash(node).await
    }

    async fn cursor(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> Result<Option<EdgeCursor>, StateError> {
        self.inner.cursor(state_instance, parent, child).await
    }

    async fn ledger_after(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        after_seq: i64,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        self.inner
            .ledger_after(state_instance, parent, after_seq)
            .await
    }

    async fn latest_seq(
        &self,
        state_instance: &str,
        parent: &PipeRef,
    ) -> Result<Option<i64>, StateError> {
        self.inner.latest_seq(state_instance, parent).await
    }
}

/// The Unix epoch, the default `created` of hand-written ledger entries.
pub fn epoch() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_state_round_trip() {
        let s = MemoryState::default();
        let p = PipeRef::new("sql:a", "x", "raw", None);
        let c = PipeRef::new("sql:b", "x", "agg", None);
        let written = s
            .append_ledger(
                "sql:b",
                &p,
                vec![(Some(Bound::Int(0)), Some(Bound::Int(10)))],
            )
            .await
            .unwrap();
        assert_eq!(written[0].seq, 1);
        s.append_ledger("sql:b", &p, vec![(None, None)])
            .await
            .unwrap();
        assert_eq!(s.latest_seq("sql:b", &p).await.unwrap(), Some(2));
        assert_eq!(s.ledger_after("sql:b", &p, 1).await.unwrap().len(), 1);
        assert!(s.ledger_after("sql:a", &p, 0).await.unwrap().is_empty());
        s.put_cursor(
            "sql:b",
            EdgeCursor {
                parent_ref: p.clone(),
                child_ref: c.clone(),
                seq: 2,
                parent_watermark_at_cursor: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(s.cursor("sql:b", &p, &c).await.unwrap().unwrap().seq, 2);
        assert_eq!(s.prune_ledger("sql:b", &p, 1).await.unwrap(), 1);
        s.set_unreachable("sql:a", true);
        assert!(matches!(
            s.watermark(&p).await,
            Err(StateError::Unreachable { .. })
        ));
        let cached = CachedState::new(&s);
        s.set_unreachable("sql:a", false);
        s.set_watermark(&p, Some(Bound::Int(5)));
        assert_eq!(cached.watermark(&p).await.unwrap(), Some(Bound::Int(5)));
        s.set_watermark(&p, Some(Bound::Int(6)));
        assert_eq!(cached.watermark(&p).await.unwrap(), Some(Bound::Int(5)));
        cached.invalidate(&p);
        assert_eq!(cached.watermark(&p).await.unwrap(), Some(Bound::Int(6)));
    }
}
