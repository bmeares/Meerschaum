//! The native [`SyncExecutor`] (D11): runs one planned node through [`crate::sync::sync_pipe`]
//! against the connector traits, writing the dirty-range ledger **before** each data commit
//! (§5.4, §5.8 write ordering).
//!
//! - `root` syncs with v4 semantics (`begin=''` or the user's bounds).
//! - `incremental` / `backfill` sync each planned chunk (or each dirty range) with explicit
//!   bounds; one ledger entry set per chunk.
//! - `rebuild` clears the pipe, then syncs the capped full range.
//!
//! The executor never advances cursors: [`crate::executor::execute`] does that after the node
//! succeeds. Python pipes reach this executor through the worker agent's `PythonConnector`
//! (a [`meerschaum_connector::Connector`]) or a [`crate::registry::PluginSync`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use meerschaum_connector::{DataQuery, async_trait};
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::success::{Bound, ChangedInterval, SyncResult};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;

use crate::axis::{Axis, AxisKind};
use crate::events::{ChunkSource, Emitter, EventKind};
use crate::executor::{NodeEvents, SyncExecutor, SyncRequest};
use crate::graph::Graph;
use crate::ledger::ledger_intervals;
use crate::node::PipeRef;
use crate::plan::{Action, Begin, Span};
use crate::registry::ConnectorRegistry;
use crate::state::StateWriter;
use crate::stream::{FlushTrigger, LegacySink, StreamFlush};
use crate::sync::{DeltaObserver, SyncArgs, SyncBegin, SyncEnv, sync_pipe};

/// Test hooks that simulate a crash at the two points the write ordering protects.
pub trait FaultInjector: Send + Sync {
    /// After the ledger entries are written, before the data commit. `Err` aborts the sync
    /// with nothing committed.
    fn after_ledger_write(&self, node: &PipeRef) -> Result<(), String> {
        let _ = node;
        Ok(())
    }

    /// After each data commit inside a node's sync (every window, chunk or stream flush),
    /// before the sync returns. `Err` fails the sync with that commit kept; later windows of
    /// the node do not run.
    fn after_data_commit(&self, node: &PipeRef) -> Result<(), String> {
        let _ = node;
        Ok(())
    }

    /// After the whole node synced successfully (every window committed, hooks run), right
    /// before the executor advances the node's edge cursors and definition hash (graph runs;
    /// in flat runs there is nothing to advance and it fires at the same point). `Err` fails
    /// the node with all of its data committed and no cursor moved. Not called for stream
    /// flushes, which advance no cursor.
    fn before_cursor_advance(&self, node: &PipeRef) -> Result<(), String> {
        let _ = node;
        Ok(())
    }
}

/// The sync options an engine run applies to every node (`sync pipes` flags).
#[derive(Debug, Clone)]
pub struct NodeSyncOptions {
    /// Additional CLI and plugin-specific keyword arguments.
    pub extra: Map,
    /// `--skip-check-existing` inverted.
    pub check_existing: bool,
    /// `--chunksize`.
    pub chunksize: Option<i64>,
    /// `--force`.
    pub force: bool,
    /// `retries` under `--force`.
    pub retries: u32,
    /// `--min-seconds` between forced retries.
    pub min_seconds: f64,
    /// `enforce_dtypes`.
    pub enforce_dtypes: bool,
    /// `params` passed to `fetch`.
    pub params: Option<Map>,
}

impl Default for NodeSyncOptions {
    fn default() -> Self {
        NodeSyncOptions {
            extra: Map::new(),
            check_existing: true,
            chunksize: Some(-1),
            force: false,
            retries: 10,
            min_seconds: 1.0,
            enforce_dtypes: true,
            params: None,
        }
    }
}

/// Configuration of a [`NativeExecutor`].
#[derive(Debug, Clone)]
pub struct NativeConfig {
    /// Pipe defaults from config.
    pub defaults: PipeDefaults,
    /// `pipes:parameters:fetch:backtrack_minutes`.
    pub default_backtrack_minutes: i64,
    /// `pipes:sync:filter_params_index_limit`.
    pub filter_params_index_limit: usize,
    /// Write ledger entries for nodes with children.
    pub write_ledger: bool,
    /// Per-node sync options.
    pub sync: NodeSyncOptions,
}

impl Default for NativeConfig {
    fn default() -> Self {
        NativeConfig {
            defaults: PipeDefaults::default(),
            default_backtrack_minutes: 1440,
            filter_params_index_limit: 250,
            write_ledger: true,
            sync: NodeSyncOptions::default(),
        }
    }
}

/// The datetime-axis positions of a frame's datetime column on `axis`.
pub fn frame_positions(df: &Frame, axis: &Axis) -> Vec<i64> {
    let Some(col) = axis.column.as_deref() else {
        return Vec::new();
    };
    let Ok(data) = df.data(col) else {
        return Vec::new();
    };
    data.values()
        .into_iter()
        .filter_map(|v| match (axis.kind, v) {
            (AxisKind::Datetime, KValue::Datetime(d)) => Some(d.utc.and_utc().timestamp_micros()),
            (AxisKind::Datetime, KValue::Date(d)) => d
                .and_hms_opt(0, 0, 0)
                .map(|n| n.and_utc().timestamp_micros()),
            (AxisKind::Int, KValue::Int(i)) => Some(i),
            (AxisKind::Int, KValue::Float(f)) if f.is_finite() => Some(f as i64),
            _ => None,
        })
        .collect()
}

/// Writes a sync's ledger entries before its data commit (the [`DeltaObserver`] the native
/// executor and stream flushes use).
pub struct LedgerWriter<'a, S> {
    /// The state writer.
    pub state: &'a S,
    /// The node.
    pub pipe_ref: PipeRef,
    /// The node's axis (for grid cells).
    pub axis: Axis,
    /// Where to write (own instance plus cross-instance children's).
    pub instances: Vec<String>,
    /// Crash injection.
    pub faults: Option<&'a dyn FaultInjector>,
    /// The intervals written, for the node's [`SyncResult::changed_intervals`].
    pub written: Mutex<Vec<ChangedInterval>>,
    /// The highest sequence number written on each state instance.
    pub max_seq: Mutex<HashMap<String, i64>>,
}

impl<'a, S: StateWriter> LedgerWriter<'a, S> {
    /// A writer for one node.
    pub fn new(
        state: &'a S,
        pipe_ref: PipeRef,
        axis: Axis,
        instances: Vec<String>,
        faults: Option<&'a dyn FaultInjector>,
    ) -> Self {
        LedgerWriter {
            state,
            pipe_ref,
            axis,
            instances,
            faults,
            written: Mutex::new(Vec::new()),
            max_seq: Mutex::new(HashMap::new()),
        }
    }

    fn take_written(&self) -> Vec<ChangedInterval> {
        std::mem::take(&mut *self.written.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Append ledger entries for `intervals` on every state instance (then crash injection).
    async fn write_intervals(&self, intervals: Vec<crate::axis::Interval>) -> Result<(), String> {
        if !intervals.is_empty() {
            let spans: Vec<(Option<Bound>, Option<Bound>)> = intervals
                .iter()
                .map(|iv| {
                    (
                        iv.begin.and_then(|b| self.axis.bound(b)),
                        iv.end.and_then(|e| self.axis.bound(e)),
                    )
                })
                .collect();
            for instance in &self.instances {
                let entries = self
                    .state
                    .append_ledger(instance, &self.pipe_ref, spans.clone())
                    .await
                    .map_err(|e| format!("cannot write the ledger on '{instance}': {e}"))?;
                if let Some(seq) = entries.iter().map(|e| e.seq).max() {
                    let mut max = self.max_seq.lock().unwrap_or_else(|e| e.into_inner());
                    let slot = max.entry(instance.clone()).or_insert(seq);
                    *slot = (*slot).max(seq);
                }
            }
            self.written
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(spans.iter().map(|(begin, end)| ChangedInterval {
                    begin: *begin,
                    end: *end,
                }));
        }
        if let Some(f) = self.faults {
            f.after_ledger_write(&self.pipe_ref)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<S: StateWriter> DeltaObserver for LedgerWriter<'_, S> {
    async fn before_commit(&self, _pipe: &Pipe, delta: &Frame) -> Result<(), String> {
        let intervals = if delta.height() > 0 {
            ledger_intervals(frame_positions(delta, &self.axis), &self.axis)
        } else {
            Vec::new()
        };
        self.write_intervals(intervals).await
    }

    /// The whole replaced window is dirty: its rows may be deleted without a replacement.
    async fn before_replace(
        &self,
        _pipe: &Pipe,
        delta: &Frame,
        begin: Option<&meerschaum_connector::DateBound>,
        end: Option<&meerschaum_connector::DateBound>,
    ) -> Result<(), String> {
        let intervals = crate::sync::replaced_intervals(
            &self.axis,
            frame_positions(delta, &self.axis),
            begin,
            end,
        );
        self.write_intervals(intervals).await
    }

    async fn after_commit(&self, _pipe: &Pipe, _result: &SyncResult) -> Result<(), String> {
        if let Some(f) = self.faults {
            f.after_data_commit(&self.pipe_ref)?;
        }
        Ok(())
    }
}

/// Faults only (no ledger): crash injection for nodes without children.
struct FaultsOnly<'a> {
    pipe_ref: PipeRef,
    faults: &'a dyn FaultInjector,
}

#[async_trait]
impl DeltaObserver for FaultsOnly<'_> {
    async fn before_commit(&self, _pipe: &Pipe, _delta: &Frame) -> Result<(), String> {
        self.faults.after_ledger_write(&self.pipe_ref)
    }

    async fn after_commit(&self, _pipe: &Pipe, _result: &SyncResult) -> Result<(), String> {
        self.faults.after_data_commit(&self.pipe_ref)
    }
}

/// Turns the fetched-chunk callbacks of a sync into `chunk_started` / `chunk_finished` events
/// (source `fetch`, §5.7), numbered in fetch order across the node's whole sync, and forwards
/// everything else to the wrapped observer (ledger writer, crash injection).
struct FetchProgress<'a> {
    inner: Option<&'a dyn DeltaObserver>,
    events: &'a NodeEvents,
    next: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl DeltaObserver for FetchProgress<'_> {
    async fn before_commit(&self, pipe: &Pipe, delta: &Frame) -> Result<(), String> {
        match self.inner {
            Some(o) => o.before_commit(pipe, delta).await,
            None => Ok(()),
        }
    }

    async fn before_replace(
        &self,
        pipe: &Pipe,
        delta: &Frame,
        begin: Option<&meerschaum_connector::DateBound>,
        end: Option<&meerschaum_connector::DateBound>,
    ) -> Result<(), String> {
        match self.inner {
            Some(o) => o.before_replace(pipe, delta, begin, end).await,
            None => Ok(()),
        }
    }

    async fn after_commit(&self, pipe: &Pipe, result: &SyncResult) -> Result<(), String> {
        match self.inner {
            Some(o) => o.after_commit(pipe, result).await,
            None => Ok(()),
        }
    }

    fn records_deltas(&self) -> bool {
        self.inner.is_some_and(|o| o.records_deltas())
    }

    fn chunk_started(&self, chunk: &crate::sync::FetchChunk) {
        let index = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.events.emit(EventKind::ChunkStarted {
            chunk: index,
            begin: chunk.begin,
            end: chunk.end,
            source: ChunkSource::Fetch,
            rows: Some(chunk.rows),
            total: None,
        });
    }

    fn chunk_finished(
        &self,
        chunk: &crate::sync::FetchChunk,
        result: &SyncResult,
        elapsed: std::time::Duration,
    ) {
        // `chunk_started` took the index, so this chunk is the last one handed out.
        let index = self
            .next
            .load(std::sync::atomic::Ordering::SeqCst)
            .saturating_sub(1);
        self.events.emit(EventKind::ChunkFinished {
            chunk: index,
            success: result.success,
            message: result.message.clone(),
            source: ChunkSource::Fetch,
            rows: Some(chunk.rows),
            inserted: result.inserted,
            updated: result.updated,
            upserted: result.upserted,
            begin: chunk.begin,
            end: chunk.end,
            duration_ms: Some(elapsed.as_millis() as u64),
        });
    }
}

/// The native node runner.
pub struct NativeExecutor<S> {
    registry: Arc<dyn ConnectorRegistry>,
    graph: Arc<Graph>,
    state: Arc<S>,
    known: Arc<HashMap<PipeRef, Map>>,
    config: NativeConfig,
    flat: bool,
    faults: Option<Arc<dyn FaultInjector>>,
}

impl<S> std::fmt::Debug for NativeExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeExecutor")
            .field("config", &self.config)
            .field("flat", &self.flat)
            .finish_non_exhaustive()
    }
}

impl<S: StateWriter + 'static> NativeExecutor<S> {
    /// A runner over `graph` (shared with the run), writing state through `state`.
    pub fn new(
        registry: Arc<dyn ConnectorRegistry>,
        graph: Arc<Graph>,
        state: Arc<S>,
        config: NativeConfig,
        flat: bool,
    ) -> NativeExecutor<S> {
        let known = graph
            .nodes()
            .map(|n| (n.pipe_ref.clone(), n.record.parameters.clone()))
            .collect();
        NativeExecutor {
            registry,
            graph,
            state,
            known: Arc::new(known),
            config,
            flat,
            faults: None,
        }
    }

    /// Inject crashes (tests).
    pub fn with_faults(mut self, faults: Arc<dyn FaultInjector>) -> Self {
        self.faults = Some(faults);
        self
    }

    fn env(&self) -> SyncEnv<'_> {
        SyncEnv {
            registry: self.registry.as_ref(),
            defaults: &self.config.defaults,
            default_backtrack_minutes: self.config.default_backtrack_minutes,
            filter_params_index_limit: self.config.filter_params_index_limit,
            known: Some(&self.known),
        }
    }

    /// Whether a node's syncs write ledger entries: it has children in the graph (in or out
    /// of the selection) and ledger writes are enabled.
    fn writes_ledger(&self, pipe_ref: &PipeRef) -> bool {
        self.config.write_ledger && self.graph.child_edges(pipe_ref).next().is_some()
    }

    fn base_args(&self) -> SyncArgs {
        let s = &self.config.sync;
        SyncArgs {
            extra: s.extra.clone(),
            check_existing: s.check_existing,
            chunksize: s.chunksize,
            force: s.force,
            retries: s.retries,
            min_seconds: s.min_seconds,
            enforce_dtypes: s.enforce_dtypes,
            params: s.params.clone(),
            parameters_loaded: true,
            ..Default::default()
        }
    }

    /// Sync one window of a node.
    async fn sync_window(
        &self,
        pipe: &Pipe,
        pipe_ref: &PipeRef,
        input: Option<PipeInput>,
        args: &SyncArgs,
        ledger: Option<&LedgerWriter<'_, S>>,
        events: Option<&NodeEvents>,
    ) -> SyncResult {
        let faults_only = match (ledger, self.faults.as_deref()) {
            (None, Some(f)) => Some(FaultsOnly {
                pipe_ref: pipe_ref.clone(),
                faults: f,
            }),
            _ => None,
        };
        let observer: Option<&dyn DeltaObserver> = match (ledger, faults_only.as_ref()) {
            (Some(l), _) => Some(l),
            (None, Some(f)) => Some(f),
            (None, None) => None,
        };
        let progress = events.map(|e| FetchProgress {
            inner: observer,
            events: e,
            next: std::sync::atomic::AtomicUsize::new(0),
        });
        let observer: Option<&dyn DeltaObserver> = match progress.as_ref() {
            Some(p) => Some(p),
            None => observer,
        };
        self.sync_observed(pipe, input, args, observer).await
    }

    fn ledger_for(&self, pipe_ref: &PipeRef, axis: &Axis) -> Option<LedgerWriter<'_, S>> {
        self.writes_ledger(pipe_ref).then(|| {
            LedgerWriter::new(
                self.state.as_ref(),
                pipe_ref.clone(),
                axis.clone(),
                self.graph.ledger_instances(pipe_ref),
                self.faults.as_deref(),
            )
        })
    }

    /// Sync rows pushed by a stream source (one micro-batch), with the ledger written first.
    /// The result's `begin` / `end` are the smallest and largest datetime-axis values of the
    /// rows the flush changed (for `stream_flushed{begin, end}`).
    pub async fn sync_rows(
        &self,
        pipe_ref: &PipeRef,
        input: PipeInput,
        check_existing: bool,
    ) -> SyncResult {
        let Some(node) = self.graph.node(pipe_ref) else {
            return SyncResult::new(false, format!("{} is not in the graph.", pipe_ref.label()));
        };
        let mut args = self.base_args();
        args.check_existing = check_existing;
        self.sync_flush(&node.pipe, pipe_ref, Some(input), &args, true)
            .await
    }

    /// Sync one `pipe.sync()` a v4-pattern ([`crate::stream::LegacyStream`]) connector's
    /// callback made, with the plugin's own arguments. A pipe in the graph gets the ledger
    /// written before the commit (like a `subscribe` flush); any other pipe is a bare
    /// `pipe.sync()` (R1). Neither runs the sync hooks: v4's `Pipe.sync()` does not.
    pub async fn sync_callback(
        &self,
        pipe: &Pipe,
        input: Option<PipeInput>,
        args: &SyncArgs,
    ) -> SyncResult {
        let pipe_ref = PipeRef::from_pipe(pipe);
        if self.graph.node(&pipe_ref).is_none() {
            let env = SyncEnv {
                known: None,
                ..self.env()
            };
            return sync_pipe(&env, pipe, input, args, None).await;
        }
        self.sync_flush(pipe, &pipe_ref, input, args, false).await
    }

    /// One flush of a graph node: ledger first (when the node has children), crash injection,
    /// and the changed rows' extent on the node's axis as the result's `begin` / `end`.
    async fn sync_flush(
        &self,
        pipe: &Pipe,
        pipe_ref: &PipeRef,
        input: Option<PipeInput>,
        args: &SyncArgs,
        hooks: bool,
    ) -> SyncResult {
        let Some(node) = self.graph.node(pipe_ref) else {
            return SyncResult::new(false, format!("{} is not in the graph.", pipe_ref.label()));
        };
        let ledger = self.ledger_for(pipe_ref, &node.axis);
        let faults_only = match (&ledger, self.faults.as_deref()) {
            (None, Some(f)) => Some(FaultsOnly {
                pipe_ref: pipe_ref.clone(),
                faults: f,
            }),
            _ => None,
        };
        let inner: Option<&dyn DeltaObserver> = match (&ledger, faults_only.as_ref()) {
            (Some(l), _) => Some(l),
            (None, Some(f)) => Some(f),
            (None, None) => None,
        };
        let extent = Extent {
            inner,
            axis: &node.axis,
            span: Mutex::new(None),
        };
        let mut result = if hooks {
            self.sync_observed(pipe, input, args, Some(&extent)).await
        } else {
            sync_pipe(&self.env(), pipe, input, args, Some(&extent)).await
        };
        if let Some(l) = &ledger {
            result.changed_intervals = l.take_written();
        }
        if let Some((lo, hi)) = *extent.span.lock().unwrap_or_else(|e| e.into_inner()) {
            result.begin = node.axis.bound(lo);
            result.end = node.axis.bound(hi);
        }
        result
    }

    /// A sync with the registry's hooks around it (unless `skip_hooks`).
    async fn sync_observed(
        &self,
        pipe: &Pipe,
        input: Option<PipeInput>,
        args: &SyncArgs,
        observer: Option<&dyn DeltaObserver>,
    ) -> SyncResult {
        let env = self.env();
        let hooks = if args.extra.get("skip_hooks").is_some_and(Value::truthy) {
            None
        } else {
            self.registry.hooks()
        };
        if let Some(h) = &hooks {
            h.pre_sync(pipe, args).await;
        }
        let result = sync_pipe(&env, pipe, input, args, observer).await;
        if let Some(h) = &hooks {
            h.post_sync(pipe, args, &result).await;
        }
        result
    }
}

/// Records the smallest and largest datetime-axis value of a sync's delta (the rows it
/// changes) and forwards everything else to the wrapped observer (ledger writer, crash
/// injection).
struct Extent<'a> {
    inner: Option<&'a dyn DeltaObserver>,
    axis: &'a Axis,
    span: Mutex<Option<(i64, i64)>>,
}

impl Extent<'_> {
    fn widen(&self, positions: &[i64]) {
        if let (Some(lo), Some(hi)) = (positions.iter().min(), positions.iter().max()) {
            let mut span = self.span.lock().unwrap_or_else(|e| e.into_inner());
            *span = Some(match *span {
                Some((a, b)) => (a.min(*lo), b.max(*hi)),
                None => (*lo, *hi),
            });
        }
    }
}

#[async_trait]
impl DeltaObserver for Extent<'_> {
    async fn before_commit(&self, pipe: &Pipe, delta: &Frame) -> Result<(), String> {
        self.widen(&frame_positions(delta, self.axis));
        match self.inner {
            Some(o) => o.before_commit(pipe, delta).await,
            None => Ok(()),
        }
    }

    /// A replaced window changes as a whole: its bounded sides widen the extent too.
    async fn before_replace(
        &self,
        pipe: &Pipe,
        delta: &Frame,
        begin: Option<&meerschaum_connector::DateBound>,
        end: Option<&meerschaum_connector::DateBound>,
    ) -> Result<(), String> {
        let mut positions = frame_positions(delta, self.axis);
        for b in [begin, end].into_iter().flatten() {
            if let Some(p) = crate::sync::date_bound_to_bound(b).and_then(|b| self.axis.pos(&b)) {
                positions.push(p);
            }
        }
        self.widen(&positions);
        match self.inner {
            Some(o) => o.before_replace(pipe, delta, begin, end).await,
            None => Ok(()),
        }
    }

    async fn after_commit(&self, pipe: &Pipe, result: &SyncResult) -> Result<(), String> {
        match self.inner {
            Some(o) => o.after_commit(pipe, result).await,
            None => Ok(()),
        }
    }
}

fn span_args(base: &SyncArgs, span: &Span) -> SyncArgs {
    let mut args = base.clone();
    args.begin = match span.begin {
        Some(b) => SyncBegin::At(b.into()),
        None => SyncBegin::Unbounded,
    };
    args.end = span.end.map(Into::into);
    args
}

fn combine(pipe: &Pipe, results: Vec<SyncResult>) -> SyncResult {
    if results.len() == 1 {
        return results.into_iter().next().expect("one result");
    }
    let n = results.len();
    let ok = results.iter().filter(|r| r.success).count();
    let mut out = SyncResult::new(
        ok == n,
        format!(
            "Synced {n} chunk{} to {pipe}\n({ok} succeeded, {} failed):\n\n{}",
            if n != 1 { "s" } else { "" },
            n - ok,
            results
                .iter()
                .map(|r| r.message.trim().to_string())
                .collect::<Vec<_>>()
                .join("\n\n")
        )
        .trim()
        .to_string(),
    );
    for r in &results {
        out.inserted += r.inserted;
        out.updated += r.updated;
        out.upserted += r.upserted;
        out.deleted += r.deleted;
    }
    out
}

impl<S: StateWriter + 'static> SyncExecutor for NativeExecutor<S> {
    fn supports_chunk_pipelining(&self) -> bool {
        !self.flat
    }

    fn before_cursor_advance(&self, node: &PipeRef) -> Result<(), String> {
        match &self.faults {
            Some(f) => f.before_cursor_advance(node),
            None => Ok(()),
        }
    }

    async fn sync(&self, request: SyncRequest) -> SyncResult {
        let started = std::time::Instant::now();
        let Some(node) = self.graph.node(&request.pipe_ref) else {
            return SyncResult::new(
                false,
                format!("{} is not in the graph.", request.pipe_ref.label()),
            );
        };
        let pipe = node.pipe.clone();
        let pipe_ref = &request.pipe_ref;
        let mut base = self.base_args();
        // The executor records the node's sync status once, after every window and retry.
        base.status_owner = Some(pipe_ref.clone());
        let ledger = self.ledger_for(pipe_ref, &node.axis);
        let _ = self.flat;

        if request.clear && request.action == Action::Rebuild {
            let instance = match self.registry.instance(pipe.instance_keys()) {
                Ok(i) => i,
                Err(e) => return SyncResult::new(false, e),
            };
            let res = instance.clear_pipe(&pipe, &DataQuery::default()).await;
            if !res.success {
                return SyncResult::new(false, format!("Failed to clear {pipe}:\n{}", res.message));
            }
            // The cleared range is dirty for every child.
            if let Some(l) = &ledger {
                for instance in &l.instances {
                    if let Err(e) = self
                        .state
                        .append_ledger(instance, pipe_ref, vec![(None, None)])
                        .await
                    {
                        return SyncResult::new(false, format!("cannot write the ledger: {e}"));
                    }
                }
            }
        }

        let bounds = request.bounds;
        let windows: Vec<SyncArgs> = match request.action {
            Action::Root | Action::Rebuild => {
                let mut args = base.clone();
                if let Some(b) = bounds {
                    args.begin = match b.begin {
                        Begin::Default => SyncBegin::Default,
                        Begin::Unbounded => SyncBegin::Unbounded,
                        Begin::At(x) => SyncBegin::At(x.into()),
                    };
                    args.end = b.end.map(Into::into);
                }
                vec![args]
            }
            _ => {
                let spans: Vec<Span> = if !request.chunks.is_empty() {
                    request.chunks.clone()
                } else if !request.ranges.is_empty() {
                    request.ranges.clone()
                } else if let Some(b) = bounds {
                    vec![Span {
                        begin: b.begin.bound(),
                        end: b.end,
                    }]
                } else {
                    vec![Span {
                        begin: None,
                        end: None,
                    }]
                };
                spans.iter().map(|s| span_args(&base, s)).collect()
            }
        };

        let chunked = windows.len() > 1 || !request.chunks.is_empty();
        let mut results = Vec::with_capacity(windows.len());
        for (i, args) in windows.iter().enumerate() {
            let window_begin = match &args.begin {
                SyncBegin::At(b) => crate::sync::date_bound_to_bound(b),
                _ => None,
            };
            let window_end = args.end.as_ref().and_then(crate::sync::date_bound_to_bound);
            if chunked {
                request.events.emit(EventKind::ChunkStarted {
                    chunk: i,
                    begin: window_begin,
                    end: window_end,
                    source: ChunkSource::Window,
                    rows: None,
                    total: Some(windows.len()),
                });
            }
            let window_started = std::time::Instant::now();
            let r = self
                .sync_window(
                    &pipe,
                    pipe_ref,
                    None,
                    args,
                    ledger.as_ref(),
                    Some(&request.events),
                )
                .await;
            if chunked {
                request.events.emit(EventKind::ChunkFinished {
                    chunk: i,
                    success: r.success,
                    message: r.message.clone(),
                    source: ChunkSource::Window,
                    rows: None,
                    inserted: r.inserted,
                    updated: r.updated,
                    upserted: r.upserted,
                    begin: window_begin,
                    end: window_end,
                    duration_ms: Some(window_started.elapsed().as_millis() as u64),
                });
            }
            let failed = !r.success;
            if !failed {
                // Chunk pipelining: this window's data is committed, so its ledger entries
                // are safe for a child to consume.
                if let (Some(progress), Some(l)) = (&request.progress, &ledger) {
                    let seqs = l.max_seq.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    progress.commit(pipe_ref, seqs);
                }
            }
            results.push(r);
            if failed {
                // Later chunks would advance past a hole; stop and let the retry policy rerun.
                break;
            }
        }
        let mut result = combine(&pipe, results);
        if let Some(l) = &ledger {
            result.changed_intervals = l.take_written();
        }
        if let Some(b) = bounds {
            result.begin = b.begin.bound();
            result.end = b.end;
        }
        result.duration = Some(started.elapsed());
        result
    }
}

/// Flushes stream micro-batches through a [`NativeExecutor`].
pub struct NativeFlush<S> {
    executor: Arc<NativeExecutor<S>>,
}

impl<S> NativeFlush<S> {
    /// Flush through `executor`.
    pub fn new(executor: Arc<NativeExecutor<S>>) -> NativeFlush<S> {
        NativeFlush { executor }
    }
}

#[async_trait]
impl<S: StateWriter + 'static> StreamFlush for NativeFlush<S> {
    async fn flush(&self, pipe: &Pipe, input: PipeInput, check_existing: bool) -> SyncResult {
        self.executor
            .sync_rows(&PipeRef::from_pipe(pipe), input, check_existing)
            .await
    }
}

/// The engine's [`LegacySink`]: runs a v4-pattern connector's callback syncs through a
/// [`NativeExecutor`] (ledger first) and reports each sync of a graph node as a
/// `stream_flushed` event with trigger [`FlushTrigger::Callback`], which drives children
/// triggering exactly like a `subscribe` flush.
///
/// It also runs the sync hooks around the connector's own `sync()` call and reports its
/// return as the pipe's lap outcome ([`LegacySink::starting`] / [`LegacySink::returned`]), as
/// v4's `sync pipes --listen` lap did.
pub struct NativeLegacySink<S> {
    executor: Arc<NativeExecutor<S>>,
    emitter: Option<Emitter>,
    outcomes: Option<tokio::sync::mpsc::UnboundedSender<crate::engine::NodeOutcome>>,
    /// The arguments of the current `sync()` call (its hooks share one `sync_timestamp`).
    calls: Mutex<HashMap<PipeRef, (SyncArgs, std::time::Instant)>>,
}

impl<S> NativeLegacySink<S> {
    /// Report flushes through `emitter` (the stream tasks' emitter).
    pub fn new(executor: Arc<NativeExecutor<S>>, emitter: Option<Emitter>) -> NativeLegacySink<S> {
        NativeLegacySink {
            executor,
            emitter,
            outcomes: None,
            calls: Mutex::new(HashMap::new()),
        }
    }

    /// Also send each `sync()` return as a lap outcome.
    pub fn with_outcomes(
        mut self,
        outcomes: tokio::sync::mpsc::UnboundedSender<crate::engine::NodeOutcome>,
    ) -> Self {
        self.outcomes = Some(outcomes);
        self
    }
}

impl<S: StateWriter + 'static> NativeLegacySink<S> {
    fn hooks(&self, args: &SyncArgs) -> Option<Arc<dyn crate::registry::SyncHooks>> {
        if args.extra.get("skip_hooks").is_some_and(Value::truthy) {
            None
        } else {
            self.executor.registry.hooks()
        }
    }
}

/// Rows in a sync's input.
fn input_rows(input: &PipeInput) -> u64 {
    match input {
        PipeInput::Frame(f) => f.height() as u64,
        PipeInput::Docs(d) => d.len() as u64,
        PipeInput::Columns(c) => c.values().map(Vec::len).max().unwrap_or(0) as u64,
    }
}

#[async_trait]
impl<S: StateWriter + 'static> LegacySink for NativeLegacySink<S> {
    async fn sync(&self, pipe: &Pipe, input: Option<PipeInput>, args: &SyncArgs) -> SyncResult {
        let pipe_ref = PipeRef::from_pipe(pipe);
        let in_graph = self.executor.graph.node(&pipe_ref).is_some();
        let rows = input.as_ref().map(input_rows);
        let result = self.executor.sync_callback(pipe, input, args).await;
        if let (true, Some(em)) = (in_graph, &self.emitter) {
            em.emit(
                Some(&pipe_ref),
                EventKind::StreamFlushed {
                    // `pipe.sync()` without a DataFrame fetched its rows itself.
                    rows: rows.unwrap_or(result.inserted + result.updated + result.upserted),
                    bytes: 0,
                    trigger: FlushTrigger::Callback,
                    success: result.success,
                    begin: result.begin,
                    end: result.end,
                    inserted: result.inserted,
                    updated: result.updated,
                    upserted: result.upserted,
                    message: result.message.clone(),
                },
            );
        }
        result
    }

    async fn starting(&self, pipe: &Pipe) {
        let args = SyncArgs {
            sync_timestamp: chrono::Utc::now(),
            ..self.executor.base_args()
        };
        if let Some(h) = self.hooks(&args) {
            h.pre_sync(pipe, &args).await;
        }
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(PipeRef::from_pipe(pipe), (args, std::time::Instant::now()));
    }

    async fn returned(&self, pipe: &Pipe, mut result: SyncResult) {
        let pipe_ref = PipeRef::from_pipe(pipe);
        let call = self
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&pipe_ref);
        let (args, started) =
            call.unwrap_or_else(|| (self.executor.base_args(), std::time::Instant::now()));
        result.duration.get_or_insert(started.elapsed());
        if let Some(h) = self.hooks(&args) {
            h.post_sync(pipe, &args, &result).await;
        }
        if let Some(tx) = &self.outcomes {
            let label = self
                .executor
                .graph
                .node(&pipe_ref)
                .map_or_else(|| pipe.to_string(), |n| n.pipe.to_string());
            let _ = tx.send(crate::engine::NodeOutcome {
                pipe_ref,
                pipe: label,
                result,
            });
        }
    }
}
