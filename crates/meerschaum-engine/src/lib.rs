//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-engine`: the dependency-aware Syncing Engine (`docs/design/v5.0.0.md` §5).
//!
//! The engine understands the pipe graph, decides which syncs need to run and how wide their
//! bounds must be, runs independent branches in parallel, and records what happened. This crate
//! runs against connector traits; [`python`] adds the host's shared worker pool and manifest:
//!
//! | Module | Spec | What it does |
//! |---|---|---|
//! | [`node`] | §5.1 | [`PipeRef`] node identity and [`PipeRecord`] (one `fetch_pipes_keys` row) |
//! | [`graph`] | §5.1 | The implicit pipe graph: declared and `{{ Pipe() }}` edges, cycles, stale references |
//! | [`selection`] | §5.2, D4, D5 | [`Selection`] / [`Pipeline`]: targets plus traversal depths, key filters |
//! | [`cadence`] | §5.5 | `parameters['schedule']`: per-pipe fire times for `--loop` (parsed by the host) |
//! | [`axis`] | §5.4 | Datetime/integer axes, half-open intervals, interval union, epoch-aligned grids |
//! | [`ledger`] | §5.4 | Ledger entries, edge cursors, range mapping across edges, compaction, definition hash |
//! | [`state`] | §5.3, §5.8 | [`StateSource`] / [`StateWriter`] traits and an in-memory implementation |
//! | [`mod@plan`] | §5.3 | The planner: one [`Action`] per node with bounds, chunks and a reason |
//! | [`schedule`] | §5.5 | EWMA durations, HEFT upward rank, CPM earliest/latest/slack |
//! | [`executor`] | §5.5, D11 | Kahn ready queue on tokio, semaphores, leases, retries, failure propagation |
//! | [`events`] | §5.7 | The typed event stream and [`EventSink`] |
//! | [`runs`] | §5.8 | Run history rows and the internal `mrsm:engine_*` pipes |
//! | [`stream`] | §5.6 | Micro-batching triggers for stream sources |
//! | [`viz`] | §5.9 | The static ASCII Gantt chart and DAG listing |
//! | [`python`] | §4.2, §4.4 | Python connector, hook, callback and stream adapters over the host's worker pool |
//! | [`registry`] | §4.4, D11 | [`ConnectorRegistry`]: how the engine reaches connectors and plugin seams |
//! | [`sync`] | §5.4, R1 | The native `Pipe.sync()` flow over the connector traits |
//! | [`native`] | §5.5, D11 | [`NativeExecutor`]: the [`SyncExecutor`] that runs planned nodes, ledger first |
//! | [`instance_state`] | §5.8, R5, R13 | [`InstanceState`]: engine state in the `mrsm:engine_*` pipes |
//! | [`leases`] | §5.8 | Instance leases with the machine-local file-lock fallback |
//! | [`engine`] | §5.2, §5.5, §5.7 | [`Engine`]: `run`, `plan`, event subscription, continuous mode |
//!
//! Chunk pipelining (`--chunk-pipelining`, R6) overlaps a child's chunks with its running
//! parents' (see [`executor`]).

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod axis;
pub mod cadence;
pub mod engine;
pub mod events;
pub mod executor;
pub mod graph;
pub mod instance_state;
pub mod leases;
pub mod ledger;
pub mod native;
pub mod node;
pub mod plan;
pub mod python;
pub mod read;
pub mod registry;
pub mod runs;
pub mod schedule;
pub mod selection;
pub mod state;
pub mod stream;
pub mod sync;
pub mod viz;

pub use axis::{Axis, AxisKind, Interval};
pub use cadence::{Cadence, PipeSchedules, Timer};
pub use engine::{
    Engine, EngineConfig, EventGranularity, LapEvent, LapSummary, LoopOptions, LoopResult,
    NodeOutcome, PendingRebuild, PlannedRun, ProcessEventSink, ProcessEventSinkRegistration,
    RunOptions, RunResult, add_process_event_sink, process_event_sinks,
};
pub use events::{ChunkSource, Event, EventKind, EventSink};
pub use executor::{
    ChunkProgress, ExecuteContext, ExecutorOptions, LeaseSource, Limits, Priority, RetryPolicy,
    SyncExecutor, SyncRequest, drain_lease_releases, execute,
};
pub use graph::{Edge, EdgeOptions, Graph, GraphError, GraphOptions, GraphWarning, Node};
pub use instance_state::InstanceState;
pub use leases::{FileLeases, InstanceLeases};
pub use ledger::{EdgeCursor, LedgerEntry, definition_hash};
pub use native::{FaultInjector, NativeConfig, NativeExecutor, NodeSyncOptions};
pub use node::{PipeRecord, PipeRef};
pub use plan::{Action, Begin, Bounds, Plan, PlanOptions, PlannedNode, Span, expected, plan};
pub use read::{
    DirtyRange, EdgeView, EngineReader, GanttBar, NodeView, PipelineView, RunDetail, RunFilter,
    RunStatus, RunSummary,
};
pub use registry::{
    ConnectorRegister, ConnectorRegisterHook, ConnectorRegistry, PluginSync, StaticRegistry,
    SyncHooks,
};
pub use runs::{NodeRow, NodeStatus, RunRecord, RunRow};
pub use schedule::{DurationSource, EwmaDurations, Schedule};
pub use selection::{Depth, KeyFilter, Pipeline, Selection, Subgraph, Targets};
pub use state::{MemoryState, StateError, StateSource, StateWriter};
pub use stream::StopToken;
pub use sync::{SyncArgs, SyncBegin};

/// Re-exported from `meerschaum-core`: a sync bound on a datetime or integer axis.
pub use meerschaum_core::success::{Bound, SyncResult};
