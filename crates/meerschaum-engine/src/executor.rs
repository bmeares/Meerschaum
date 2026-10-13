//! The executor (§5.5, D11).
//!
//! - A **dynamic ready queue** (Kahn's algorithm) on tokio: a node is ready when all of its
//!   in-selection parents finished.
//! - **Priority** is the HEFT upward rank from an EWMA [`DurationSource`]; ties go to plan order.
//! - **Concurrency limits**: the global worker count, per-connector and per-instance semaphores,
//!   and the Python worker pool. A ready node is dispatched only when every permit it needs is
//!   free, so a waiting node never holds a worker slot.
//! - **Re-planning**: a provisional node is re-planned with [`plan_node`] when its parents
//!   finish, against the state their syncs produced.
//! - **Failures** mark every descendant `upstream_failed`; independent branches keep running.
//!   Per-pipe retry policy: `parameters['retry'] = {max: 3, backoff_seconds: 30}`.
//! - **Leases**: a per-pipe lease through a [`LeaseSource`] is held (and renewed) while a node
//!   syncs, so two jobs never sync the same pipe at once.
//! - **Write ordering**: a node's edge cursors and definition hash are written only after its
//!   sync succeeds.
//!
//! Each node runs through a [`SyncExecutor`] (D11): [`crate::native::NativeExecutor`] runs the
//! native `Pipe.sync()` flow over the connector traits (Python connectors reach it through the
//! worker's `PythonConnector`); [`FakeExecutor`] serves tests. Continuous mode lives in
//! [`crate::engine::Engine::run_loop`]. Chunk pipelining (`--chunk-pipelining`, R6) is accepted
//! and ignored.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use indexmap::IndexMap;
use meerschaum_core::success::{Bound, SyncResult};
use meerschaum_core::{Map, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::events::{Emitter, EventKind, EventSink};
use crate::graph::Graph;
use crate::ledger::EdgeCursor;
use crate::node::{PipeRecord, PipeRef};
use crate::plan::{Action, Bounds, ParentState, Plan, PlanOptions, PlannedNode, Span, plan_node};
use crate::runs::{NodeRow, NodeStatus, RunRecord, RunRow, new_run_id};
use crate::schedule::{DurationSource, schedule};
use crate::selection::Subgraph;
use crate::state::{CachedState, MemoryState, StateError, StateSource, StateWriter};

/// Emits events for one node from inside a [`SyncExecutor`] (chunk progress).
#[derive(Debug, Clone)]
pub struct NodeEvents {
    emitter: Emitter,
    pipe_ref: PipeRef,
}

impl NodeEvents {
    /// Emit an event for this node.
    pub fn emit(&self, kind: EventKind) {
        self.emitter.emit(Some(&self.pipe_ref), kind);
    }
}

/// Everything a node runner needs to sync one node.
#[derive(Debug, Clone)]
pub struct SyncRequest {
    /// The run.
    pub run_id: String,
    /// The node.
    pub pipe_ref: PipeRef,
    /// The node's record (keys and parameters).
    pub record: PipeRecord,
    /// The action (`root`, `incremental`, `backfill` or `rebuild`).
    pub action: Action,
    /// Clear the pipe first (`rebuild`).
    pub clear: bool,
    /// Overall bounds.
    pub bounds: Option<Bounds>,
    /// The dirty ranges (`backfill`).
    pub ranges: Vec<Span>,
    /// The chunk list (empty: the sync chooses).
    pub chunks: Vec<Span>,
    /// 1 for the first attempt.
    pub attempt: u32,
    /// Event emitter for chunk progress.
    pub events: NodeEvents,
}

/// Runs one node's sync (D11). Implementations: the native Rust `Pipe::sync`, the Python
/// adapter, and [`FakeExecutor`].
pub trait SyncExecutor: Send + Sync + 'static {
    /// Sync one node. Failures are returned as an unsuccessful [`SyncResult`], never raised.
    fn sync(&self, request: SyncRequest) -> impl Future<Output = SyncResult> + Send;
}

/// Per-pipe leases (§5.8): an atomic acquire with a TTL, renewed while a node syncs.
pub trait LeaseSource: Send + Sync + 'static {
    /// Try to take the lease. `Ok(false)` when another holder has it.
    fn acquire(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> impl Future<Output = Result<bool, StateError>> + Send;

    /// Extend a held lease. `Ok(false)` when the lease was lost.
    fn renew(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> impl Future<Output = Result<bool, StateError>> + Send;

    /// Release a held lease (a no-op if another holder has it).
    fn release(
        &self,
        node: &PipeRef,
        holder: &str,
    ) -> impl Future<Output = Result<(), StateError>> + Send;
}

/// In-memory leases with expiry (on tokio's clock).
#[derive(Debug, Default)]
pub struct MemoryLeases {
    held: Mutex<HashMap<PipeRef, (String, tokio::time::Instant)>>,
}

impl MemoryLeases {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PipeRef, (String, tokio::time::Instant)>> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The current holder, if the lease is live.
    pub fn holder(&self, node: &PipeRef) -> Option<String> {
        let now = tokio::time::Instant::now();
        self.lock()
            .get(node)
            .filter(|(_, exp)| *exp > now)
            .map(|(h, _)| h.clone())
    }
}

impl LeaseSource for MemoryLeases {
    async fn acquire(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> Result<bool, StateError> {
        let now = tokio::time::Instant::now();
        let mut held = self.lock();
        match held.get(node) {
            Some((h, exp)) if *exp > now && h != holder => Ok(false),
            _ => {
                held.insert(node.clone(), (holder.to_string(), now + ttl));
                Ok(true)
            }
        }
    }

    async fn renew(&self, node: &PipeRef, holder: &str, ttl: Duration) -> Result<bool, StateError> {
        let now = tokio::time::Instant::now();
        let mut held = self.lock();
        match held.get_mut(node) {
            Some((h, exp)) if h == holder => {
                *exp = now + ttl;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release(&self, node: &PipeRef, holder: &str) -> Result<(), StateError> {
        let mut held = self.lock();
        if held.get(node).is_some_and(|(h, _)| h == holder) {
            held.remove(node);
        }
        Ok(())
    }
}

/// Leases that are always granted (single process, or leases disabled).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoLeases;

impl LeaseSource for NoLeases {
    async fn acquire(&self, _: &PipeRef, _: &str, _: Duration) -> Result<bool, StateError> {
        Ok(true)
    }

    async fn renew(&self, _: &PipeRef, _: &str, _: Duration) -> Result<bool, StateError> {
        Ok(true)
    }

    async fn release(&self, _: &PipeRef, _: &str) -> Result<(), StateError> {
        Ok(())
    }
}

/// Concurrency limits beyond the global worker count (`system.engine.limits`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Limits {
    /// Per connector keys (`system.engine.limits.connectors['sql:main'] = 4`).
    pub connectors: HashMap<String, usize>,
    /// Per instance keys.
    pub instances: HashMap<String, usize>,
    /// The Python worker pool size (nodes with [`PipeRecord::python`]).
    pub python_workers: Option<usize>,
}

/// A per-pipe retry policy (`parameters['retry'] = {max: 3, backoff_seconds: 30}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetryPolicy {
    /// Retries after the first attempt.
    pub max: u32,
    /// Fixed delay before each retry.
    pub backoff: Duration,
}

impl RetryPolicy {
    /// Read `parameters['retry']`; missing or invalid values mean no retries.
    pub fn from_parameters(parameters: &Map) -> RetryPolicy {
        let Some(Value::Map(r)) = parameters.get("retry") else {
            return RetryPolicy::default();
        };
        RetryPolicy {
            max: r
                .get("max")
                .and_then(Value::as_i64)
                .map_or(0, |m| u32::try_from(m.max(0)).unwrap_or(u32::MAX)),
            backoff: r
                .get("backoff_seconds")
                .and_then(Value::as_f64)
                .filter(|s| s.is_finite() && *s >= 0.0)
                .map_or(Duration::ZERO, Duration::from_secs_f64),
        }
    }
}

/// Executor options.
#[derive(Debug, Clone)]
pub struct ExecutorOptions {
    /// The global worker count.
    pub workers: usize,
    /// Per-connector, per-instance and Python limits.
    pub limits: Limits,
    /// The lease holder name (the job name or `hostname:pid`).
    pub holder: String,
    /// Lease time-to-live; renewed every third of it while syncing.
    pub lease_ttl: Duration,
    /// How long to wait for a lease held by someone else before failing the attempt.
    pub lease_wait: Duration,
    /// How often to retry a held lease.
    pub lease_poll: Duration,
    /// `--chunk-pipelining` (R6). Not implemented in this wave; ignored.
    pub chunk_pipelining: bool,
    /// Use this run ID instead of a fresh ULID.
    pub run_id: Option<String>,
    /// The planner options used to re-plan provisional nodes (must match the plan's).
    pub plan: PlanOptions,
    /// The selection as JSON, recorded on the run row for `--resume`.
    pub selection: Option<String>,
    /// Dispatch order among ready nodes.
    pub priority: Priority,
}

/// How ready nodes are ordered when more are ready than can run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Priority {
    /// HEFT upward rank (the critical path first), ties in plan order (§5.5).
    #[default]
    UpwardRank,
    /// Plan order only. Flat selections use it so `-w 1` laps sync in v4's order.
    PlanOrder,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        ExecutorOptions {
            workers: 4,
            limits: Limits::default(),
            holder: format!("mrsm:{}", std::process::id()),
            lease_ttl: Duration::from_secs(60),
            lease_wait: Duration::from_secs(30),
            lease_poll: Duration::from_millis(500),
            chunk_pipelining: false,
            run_id: None,
            plan: PlanOptions::default(),
            selection: None,
            priority: Priority::UpwardRank,
        }
    }
}

/// What [`execute`] runs against.
pub struct ExecuteContext<'a, S, E, L> {
    /// The pipe graph.
    pub graph: &'a Graph,
    /// The selection the plan was made from.
    pub subgraph: &'a Subgraph,
    /// Engine state (read for re-planning, written for cursors, hashes and run history).
    pub state: &'a S,
    /// The node runner.
    pub executor: Arc<E>,
    /// The lease source.
    pub leases: Arc<L>,
    /// The event sink.
    pub sink: Arc<dyn EventSink>,
    /// Duration estimates for priorities and CPM.
    pub durations: &'a dyn DurationSource,
}

enum TaskOutput {
    Done {
        pipe_ref: PipeRef,
        attempt: u32,
        result: SyncResult,
        started: chrono::DateTime<Utc>,
        elapsed: Duration,
    },
    Retry(PipeRef),
}

struct Dispatcher<'a, 'c, S, E, L> {
    ctx: &'c ExecuteContext<'a, S, E, L>,
    options: &'c ExecutorOptions,
    flat: bool,
    emitter: Emitter,
    cached: CachedState<'a, S>,
    planned: HashMap<PipeRef, PlannedNode>,
    rows: IndexMap<PipeRef, NodeRow>,
    outcome: HashMap<PipeRef, ParentState>,
    indegree: HashMap<PipeRef, usize>,
    children: HashMap<PipeRef, Vec<PipeRef>>,
    parents: HashMap<PipeRef, Vec<PipeRef>>,
    newly_ready: Vec<PipeRef>,
    attempts: HashMap<PipeRef, u32>,
    state_warnings: Vec<String>,
}

impl<S, E, L> Dispatcher<'_, '_, S, E, L>
where
    S: StateSource + StateWriter,
    E: SyncExecutor,
    L: LeaseSource,
{
    fn finish(&mut self, r: &PipeRef, status: NodeStatus, message: Option<String>) {
        self.outcome.insert(
            r.clone(),
            match status {
                NodeStatus::Succeeded => ParentState::Succeeded,
                NodeStatus::Failed | NodeStatus::UpstreamFailed => ParentState::Failed,
                _ => ParentState::Idle,
            },
        );
        if let Some(row) = self.rows.get_mut(r) {
            row.status = status;
            if message.is_some() {
                row.message = message;
            }
        }
        for c in self.children.get(r).cloned().unwrap_or_default() {
            if let Some(d) = self.indegree.get_mut(&c) {
                *d -= 1;
                if *d == 0 {
                    self.newly_ready.push(c);
                }
            }
        }
    }

    /// Re-plan a node whose parents finished, and settle it if it does not run.
    /// Returns `true` if the node still needs to run.
    async fn resolve(&mut self, r: &PipeRef) -> bool {
        let mut node = self.planned[r].clone();
        let parent_failed = self
            .parents
            .get(r)
            .into_iter()
            .flatten()
            .any(|p| self.outcome.get(p) == Some(&ParentState::Failed));
        if !self.flat && (node.provisional || parent_failed) {
            let states: HashMap<PipeRef, ParentState> = self
                .parents
                .get(r)
                .into_iter()
                .flatten()
                .map(|p| {
                    (
                        p.clone(),
                        self.outcome.get(p).copied().unwrap_or(ParentState::Idle),
                    )
                })
                .collect();
            let mut replanned = plan_node(
                self.ctx.graph,
                self.ctx.subgraph,
                r,
                &states,
                &self.cached,
                &self.options.plan,
            )
            .await;
            if node.action == Action::Rebuild && !node.requires_confirmation {
                // A confirmed rebuild stays confirmed.
                replanned.requires_confirmation = false;
            }
            self.emitter.emit(
                Some(r),
                EventKind::NodePlanned {
                    action: replanned.action,
                    reason: replanned.reason.clone(),
                    provisional: false,
                },
            );
            node = replanned;
            self.planned.insert(r.clone(), node.clone());
        }
        if let Some(row) = self.rows.get_mut(r) {
            row.action = node.action;
            row.reason = node.reason.clone();
            row.begin = node.bounds.as_ref().and_then(|b| b.begin.bound());
            row.end = node.bounds.as_ref().and_then(|b| b.end);
            row.num_chunks = node.chunks.len();
        }
        let (status, kind) = match node.action {
            a if a.runs() && !node.requires_confirmation => return true,
            Action::Rebuild => (
                NodeStatus::RebuildPending,
                EventKind::NodeRebuildPending {
                    reason: format!("{}; not confirmed", node.reason),
                },
            ),
            Action::RebuildPending => (
                NodeStatus::RebuildPending,
                EventKind::NodeRebuildPending {
                    reason: node.reason.clone(),
                },
            ),
            Action::Observe => (
                NodeStatus::Observed,
                EventKind::NodeSkipped {
                    reason: node.reason.clone(),
                },
            ),
            Action::UpstreamFailed => (
                NodeStatus::UpstreamFailed,
                EventKind::NodeSkipped {
                    reason: format!("upstream_failed: {}", node.reason),
                },
            ),
            _ => (
                NodeStatus::Skipped,
                EventKind::NodeSkipped {
                    reason: node.reason.clone(),
                },
            ),
        };
        if status == NodeStatus::Skipped && !self.flat && node.stored_definition_hash.is_none() {
            // Establish the baseline hash for a node whose data is current.
            self.write_hash(&node).await;
        }
        self.emitter.emit(Some(r), kind);
        self.finish(r, status, None);
        false
    }

    async fn write_hash(&mut self, node: &PlannedNode) {
        if node.stored_definition_hash.as_deref() == Some(node.definition_hash.as_str()) {
            return;
        }
        if let Err(e) = self
            .ctx
            .state
            .put_definition_hash(&node.pipe_ref, &node.definition_hash)
            .await
        {
            self.state_warnings.push(format!(
                "{}: cannot write the definition hash: {e}",
                node.pipe_ref.label()
            ));
        }
    }

    async fn on_success(&mut self, r: &PipeRef, result: &SyncResult) {
        let node = self.planned[r].clone();
        if !self.flat {
            // One write per state instance (usually one: the child's own instance).
            let mut by_instance: IndexMap<String, Vec<EdgeCursor>> = IndexMap::new();
            for c in &node.consumes {
                by_instance
                    .entry(c.state_instance.clone())
                    .or_default()
                    .push(EdgeCursor {
                        parent_ref: c.parent_ref.clone(),
                        child_ref: c.child_ref.clone(),
                        seq: c.seq,
                        parent_watermark_at_cursor: c.parent_watermark,
                    });
            }
            for (instance, cursors) in by_instance {
                let parents: Vec<String> = cursors.iter().map(|c| c.parent_ref.label()).collect();
                if let Err(e) = self.ctx.state.put_cursors(&instance, cursors).await {
                    self.state_warnings.push(format!(
                        "{}: cannot advance the cursors from {}: {e}",
                        r.label(),
                        parents.join(", ")
                    ));
                }
            }
            self.write_hash(&node).await;
        }
        self.cached.invalidate(r);
        let after = self.cached.watermark(r).await.ok().flatten();
        if after != node.watermark {
            self.emitter.emit(
                Some(r),
                EventKind::WatermarkAdvanced {
                    from: node.watermark,
                    to: after,
                },
            );
        }
        self.emitter.emit(
            Some(r),
            EventKind::NodeFinished {
                result: result.clone(),
            },
        );
    }
}

fn try_permits(
    record: &PipeRecord,
    global: &Arc<Semaphore>,
    connectors: &HashMap<String, Arc<Semaphore>>,
    instances: &HashMap<String, Arc<Semaphore>>,
    python: Option<&Arc<Semaphore>>,
) -> Option<Vec<OwnedSemaphorePermit>> {
    let mut permits = vec![global.clone().try_acquire_owned().ok()?];
    if let Some(s) = instances.get(&record.instance_keys) {
        permits.push(s.clone().try_acquire_owned().ok()?);
    }
    if let Some(s) = connectors.get(&record.connector_keys) {
        permits.push(s.clone().try_acquire_owned().ok()?);
    }
    if record.python {
        if let Some(s) = python {
            permits.push(s.clone().try_acquire_owned().ok()?);
        }
    }
    Some(permits)
}

/// Run a plan to completion and return the run record (also persisted through
/// [`StateWriter::record_run`]).
pub async fn execute<S, E, L>(
    ctx: ExecuteContext<'_, S, E, L>,
    plan: Plan,
    options: &ExecutorOptions,
) -> RunRecord
where
    S: StateSource + StateWriter,
    E: SyncExecutor,
    L: LeaseSource,
{
    let run_id = options.run_id.clone().unwrap_or_else(new_run_id);
    let emitter = Emitter::new(&run_id, ctx.sink.clone());
    let sched = schedule(&plan, ctx.durations);
    let started = Utc::now();
    let order: HashMap<PipeRef, usize> = plan
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.pipe_ref.clone(), i))
        .collect();
    let mut rows: IndexMap<PipeRef, NodeRow> = IndexMap::new();
    for n in &plan.nodes {
        let mut row = NodeRow::new(&run_id, &n.pipe_ref, n.action, &n.reason);
        if let Some(t) = sched.nodes.get(&n.pipe_ref) {
            row.planned_start_seconds = Some(t.earliest_start.as_secs_f64());
            row.slack_seconds = Some(t.slack.as_secs_f64());
        }
        rows.insert(n.pipe_ref.clone(), row);
    }
    let mut indegree: HashMap<PipeRef, usize> =
        plan.nodes.iter().map(|n| (n.pipe_ref.clone(), 0)).collect();
    let mut children: HashMap<PipeRef, Vec<PipeRef>> = HashMap::new();
    let mut parents: HashMap<PipeRef, Vec<PipeRef>> = HashMap::new();
    for (p, c) in &plan.edges {
        if order.contains_key(p) && order.contains_key(c) {
            *indegree.get_mut(c).expect("in plan") += 1;
            children.entry(p.clone()).or_default().push(c.clone());
            parents.entry(c.clone()).or_default().push(p.clone());
        }
    }
    let targets: Vec<PipeRef> = ctx.subgraph.targets.iter().cloned().collect();
    emitter.emit(
        None,
        EventKind::RunStarted {
            nodes: plan.nodes.len(),
            workers: options.workers,
            targets: targets.clone(),
        },
    );
    for n in &plan.nodes {
        emitter.emit(
            Some(&n.pipe_ref),
            EventKind::NodePlanned {
                action: n.action,
                reason: n.reason.clone(),
                provisional: n.provisional,
            },
        );
    }

    let mut d = Dispatcher {
        ctx: &ctx,
        options,
        flat: plan.flat,
        emitter: emitter.clone(),
        cached: CachedState::new(ctx.state),
        planned: plan
            .nodes
            .iter()
            .map(|n| (n.pipe_ref.clone(), n.clone()))
            .collect(),
        rows,
        outcome: HashMap::new(),
        indegree,
        children,
        parents,
        newly_ready: Vec::new(),
        attempts: HashMap::new(),
        state_warnings: Vec::new(),
    };

    let global = Arc::new(Semaphore::new(options.workers));
    let sems = |m: &HashMap<String, usize>| -> HashMap<String, Arc<Semaphore>> {
        m.iter()
            .map(|(k, v)| (k.clone(), Arc::new(Semaphore::new(*v))))
            .collect()
    };
    let connector_sems = sems(&options.limits.connectors);
    let instance_sems = sems(&options.limits.instances);
    let python_sem = options
        .limits
        .python_workers
        .map(|n| Arc::new(Semaphore::new(n)));
    let by_rank = options.priority == Priority::UpwardRank;
    let rank = |r: &PipeRef| {
        if by_rank {
            sched.nodes.get(r).map_or(Duration::ZERO, |t| t.upward_rank)
        } else {
            Duration::ZERO
        }
    };

    let mut ready: Vec<PipeRef> = plan
        .nodes
        .iter()
        .filter(|n| d.indegree[&n.pipe_ref] == 0)
        .map(|n| n.pipe_ref.clone())
        .collect();
    let mut resolved: HashSet<PipeRef> = HashSet::new();
    let mut join: JoinSet<TaskOutput> = JoinSet::new();

    loop {
        ready.sort_by(|a, b| rank(b).cmp(&rank(a)).then_with(|| order[a].cmp(&order[b])));
        let mut deferred = Vec::new();
        let mut progressed = false;
        for r in std::mem::take(&mut ready) {
            if resolved.insert(r.clone()) && !d.resolve(&r).await {
                progressed = true;
                continue;
            }
            let Some(node) = ctx.graph.node(&r) else {
                d.finish(&r, NodeStatus::Failed, Some("not in the graph".into()));
                progressed = true;
                continue;
            };
            let Some(permits) = try_permits(
                &node.record,
                &global,
                &connector_sems,
                &instance_sems,
                python_sem.as_ref(),
            ) else {
                deferred.push(r);
                continue;
            };
            progressed = true;
            let attempt = {
                let a = d.attempts.entry(r.clone()).or_insert(0);
                *a += 1;
                *a
            };
            let planned = &d.planned[&r];
            let request = SyncRequest {
                run_id: run_id.clone(),
                pipe_ref: r.clone(),
                record: node.record.clone(),
                action: planned.action,
                clear: planned.clear,
                bounds: planned.bounds,
                ranges: planned.ranges.clone(),
                chunks: planned.chunks.clone(),
                attempt,
                events: NodeEvents {
                    emitter: emitter.clone(),
                    pipe_ref: r.clone(),
                },
            };
            emitter.emit(
                Some(&r),
                EventKind::NodeStarted {
                    action: planned.action,
                    attempt,
                },
            );
            let executor = ctx.executor.clone();
            let leases = ctx.leases.clone();
            let holder = options.holder.clone();
            let (ttl, wait, poll) = (options.lease_ttl, options.lease_wait, options.lease_poll);
            join.spawn(async move {
                let _permits = permits;
                let started = Utc::now();
                let t0 = tokio::time::Instant::now();
                let done = |result: SyncResult| TaskOutput::Done {
                    pipe_ref: request.pipe_ref.clone(),
                    attempt,
                    result,
                    started,
                    elapsed: t0.elapsed(),
                };
                let deadline = t0 + wait;
                loop {
                    match leases.acquire(&request.pipe_ref, &holder, ttl).await {
                        Ok(true) => break,
                        Ok(false) if tokio::time::Instant::now() < deadline => {
                            tokio::time::sleep(poll).await;
                        }
                        Ok(false) => {
                            return done(SyncResult::new(
                                false,
                                format!(
                                    "the lease on {} is held by another process",
                                    request.pipe_ref.label()
                                ),
                            ));
                        }
                        Err(e) => {
                            return done(SyncResult::new(
                                false,
                                format!("cannot acquire the lease: {e}"),
                            ));
                        }
                    }
                }
                let pipe_ref = request.pipe_ref.clone();
                let result = {
                    let sync = executor.sync(request);
                    tokio::pin!(sync);
                    let period = (ttl / 3).max(Duration::from_millis(1));
                    let mut heartbeat =
                        tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                    loop {
                        tokio::select! {
                            result = &mut sync => break result,
                            _ = heartbeat.tick() => {
                                match leases.renew(&pipe_ref, &holder, ttl).await {
                                    Ok(true) => {},
                                    Ok(false) => break SyncResult::new(false, format!("the lease on {} was lost during sync", pipe_ref.label())),
                                    Err(e) => break SyncResult::new(false, format!("cannot renew the lease during sync: {e}")),
                                }
                            }
                        }
                    }
                }; // Drop the cancelled sync future before releasing its lease.
                let _ = leases.release(&pipe_ref, &holder).await;
                TaskOutput::Done {
                    pipe_ref,
                    attempt,
                    result,
                    started,
                    elapsed: t0.elapsed(),
                }
            });
        }
        ready = deferred;
        if !d.newly_ready.is_empty() {
            ready.append(&mut d.newly_ready);
            continue;
        }
        if progressed && !ready.is_empty() {
            continue;
        }
        if join.is_empty() {
            if ready.is_empty() {
                break;
            }
            // Nothing runs and nothing can start: a concurrency limit of zero.
            for r in std::mem::take(&mut ready) {
                d.finish(
                    &r,
                    NodeStatus::Failed,
                    Some("no worker slot available (a concurrency limit is 0)".into()),
                );
                emitter.emit(
                    Some(&r),
                    EventKind::NodeFailed {
                        error: "no worker slot available (a concurrency limit is 0)".into(),
                        traceback: None,
                        attempt: 0,
                        will_retry: false,
                    },
                );
            }
            ready.append(&mut d.newly_ready);
            continue;
        }
        let Some(joined) = join.join_next().await else {
            continue;
        };
        let output = match joined {
            Ok(o) => o,
            Err(e) => {
                // A panicking executor: the node is unknown here, so record a run warning.
                d.state_warnings.push(format!("a sync task panicked: {e}"));
                continue;
            }
        };
        match output {
            TaskOutput::Retry(r) => ready.push(r),
            TaskOutput::Done {
                pipe_ref: r,
                attempt,
                result,
                started,
                elapsed,
            } => {
                if let Some(row) = d.rows.get_mut(&r) {
                    row.attempts = attempt;
                    row.started.get_or_insert(started);
                    row.finished = Some(Utc::now());
                    row.message = Some(result.message.clone());
                }
                if result.success {
                    if let Some(row) = d.rows.get_mut(&r) {
                        row.duration_seconds =
                            Some(result.duration.unwrap_or(elapsed).as_secs_f64());
                        row.inserted = result.inserted;
                        row.updated = result.updated;
                        row.upserted = result.upserted;
                        row.deleted = result.deleted;
                    }
                    d.on_success(&r, &result).await;
                    d.finish(&r, NodeStatus::Succeeded, None);
                } else {
                    let policy = ctx
                        .graph
                        .node(&r)
                        .map(|n| RetryPolicy::from_parameters(&n.record.parameters))
                        .unwrap_or_default();
                    let will_retry = attempt <= policy.max;
                    emitter.emit(
                        Some(&r),
                        EventKind::NodeFailed {
                            error: result.message.clone(),
                            traceback: None,
                            attempt,
                            will_retry,
                        },
                    );
                    if will_retry {
                        let backoff = policy.backoff;
                        join.spawn(async move {
                            tokio::time::sleep(backoff).await;
                            TaskOutput::Retry(r)
                        });
                    } else {
                        d.finish(&r, NodeStatus::Failed, Some(result.message.clone()));
                    }
                }
                ready.append(&mut d.newly_ready);
            }
        }
    }

    let count = |s: NodeStatus| d.rows.values().filter(|r| r.status == s).count();
    let failed = count(NodeStatus::Failed);
    let upstream_failed = count(NodeStatus::UpstreamFailed);
    let pending = count(NodeStatus::RebuildPending);
    let success = failed == 0 && upstream_failed == 0 && count(NodeStatus::Pending) == 0;
    let mut message = format!(
        "{} succeeded, {} skipped, {} observed, {failed} failed, {upstream_failed} upstream_failed, \
         {pending} rebuild_pending",
        count(NodeStatus::Succeeded),
        count(NodeStatus::Skipped),
        count(NodeStatus::Observed),
    );
    for w in &d.state_warnings {
        message.push_str("\nwarning: ");
        message.push_str(w);
    }
    emitter.emit(
        None,
        EventKind::RunFinished {
            success,
            message: message.clone(),
        },
    );
    let record = RunRecord {
        run: RunRow {
            run_id: run_id.clone(),
            started,
            finished: Some(Utc::now()),
            success: Some(success),
            message: Some(message),
            targets,
            workers: options.workers,
            num_nodes: plan.nodes.len(),
            resumed_from: options.plan.resumed_from.clone(),
            holder: options.holder.clone(),
            selection: options.selection.clone(),
        },
        nodes: d.rows.into_values().collect(),
    };
    let _ = ctx.state.record_run(&record).await;
    record
}

/// One scripted attempt of the [`FakeExecutor`].
#[derive(Debug, Clone)]
pub struct FakeStep {
    /// How long the sync takes.
    pub delay: Duration,
    /// The result.
    pub result: SyncResult,
    /// Ledger entries to append on success, per state instance.
    pub ledger: Vec<(String, Option<Bound>, Option<Bound>)>,
    /// The node's watermark after a successful sync (`None` leaves it unchanged).
    pub watermark: Option<Option<Bound>>,
}

impl FakeStep {
    /// A successful step.
    pub fn ok(delay: Duration) -> FakeStep {
        FakeStep {
            delay,
            result: SyncResult::new(true, "Success"),
            ledger: Vec::new(),
            watermark: None,
        }
    }

    /// A failing step.
    pub fn fail(message: &str) -> FakeStep {
        FakeStep {
            delay: Duration::ZERO,
            result: SyncResult::new(false, message),
            ledger: Vec::new(),
            watermark: None,
        }
    }
}

/// An in-memory [`SyncExecutor`] for tests: scripted results per node, optional effects on a
/// [`MemoryState`], and a record of calls and peak concurrency.
#[derive(Debug, Default)]
pub struct FakeExecutor {
    scripts: Mutex<HashMap<PipeRef, VecDeque<FakeStep>>>,
    default_delay: Duration,
    state: Option<Arc<MemoryState>>,
    calls: Mutex<Vec<SyncRequest>>,
    running: AtomicUsize,
    peak: AtomicUsize,
}

impl FakeExecutor {
    /// Every unscripted sync succeeds after `default_delay`. Effects go to `state`, if given.
    pub fn new(default_delay: Duration, state: Option<Arc<MemoryState>>) -> FakeExecutor {
        FakeExecutor {
            default_delay,
            state,
            ..Default::default()
        }
    }

    /// Queue a step for a node (consumed one per attempt).
    pub fn script(&self, node: &PipeRef, step: FakeStep) {
        self.scripts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(node.clone())
            .or_default()
            .push_back(step);
    }

    /// The requests received, in call order.
    pub fn calls(&self) -> Vec<SyncRequest> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The nodes synced, in call order.
    pub fn synced(&self) -> Vec<PipeRef> {
        self.calls().into_iter().map(|c| c.pipe_ref).collect()
    }

    /// The highest number of concurrent syncs observed.
    pub fn peak_concurrency(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

impl SyncExecutor for FakeExecutor {
    async fn sync(&self, request: SyncRequest) -> SyncResult {
        let step = self
            .scripts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&request.pipe_ref)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| FakeStep::ok(self.default_delay));
        let pipe_ref = request.pipe_ref.clone();
        for (i, chunk) in request.chunks.iter().enumerate() {
            request.events.emit(EventKind::ChunkStarted {
                chunk: i,
                begin: chunk.begin,
                end: chunk.end,
                source: crate::events::ChunkSource::Window,
                rows: None,
                total: Some(request.chunks.len()),
            });
        }
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(request.clone());
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(step.delay).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        for (i, chunk) in request.chunks.iter().enumerate() {
            request.events.emit(EventKind::ChunkFinished {
                chunk: i,
                success: step.result.success,
                message: step.result.message.clone(),
                source: crate::events::ChunkSource::Window,
                rows: None,
                inserted: 0,
                updated: 0,
                upserted: 0,
                begin: chunk.begin,
                end: chunk.end,
                duration_ms: Some(step.delay.as_millis() as u64),
            });
        }
        if step.result.success {
            if let Some(state) = &self.state {
                for (instance, begin, end) in &step.ledger {
                    let _ = state
                        .append_ledger(instance, &pipe_ref, vec![(*begin, *end)])
                        .await;
                }
                if let Some(wm) = step.watermark {
                    state.set_watermark(&pipe_ref, wm);
                }
            }
        }
        step.result
    }
}

#[cfg(test)]
mod tests;
